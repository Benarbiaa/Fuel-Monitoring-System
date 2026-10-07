"""
Alert generation for incoming fuel readings.

Called by the ingestion consumer (ingestion_consumer/main.py) for every new
reading, inside the same database transaction as the reading itself. Lives
in services/ rather than in a route module because it's business logic with
no HTTP involved: the consumer is a separate process and shouldn't depend
on FastAPI route code.
"""

from sqlalchemy.orm import Session

from backend.database import models
from backend.schemas import FuelDataResponse
from backend.services import storage


def _append_alert(db: Session, station_id: str, fuel_type: str, alert_type: str, severity: str, message: str) -> None:
    """
    Adds a new alert to the current transaction via the storage service.
    """
    storage.create_alert(
        db=db,
        station_id=station_id,
        fuel_type=fuel_type,
        alert_type=alert_type,
        severity=severity,
        message=f"[{severity.upper()}] {message}" 
    )
# Minimum stock increase (liters) between two consecutive readings for the
# same station/fuel_type before we treat it as a delivery rather than noise.
# In this system stock only ever decreases from sales (see the station
# simulator), so any real increase can only mean a delivery truck arrived —
# there's no explicit "delivery" flag in the telemetry, exactly like a real
# tank sensor wouldn't report one; we infer it from the data itself.
RESTOCK_MIN_INCREASE_LITERS = 50.0


def _check_restock(db: Session, record: FuelDataResponse) -> int:
    """
    Compares this reading against the immediately preceding reading for the
    same station/fuel_type. If stock jumped up by a meaningful amount, logs
    an informational RESTOCK alert so the delivery is visible in the app
    (dashboard alerts feed, chat assistant, automation agent) instead of
    only being observable as a silent jump in the stock history chart.
    """
    previous = (
        db.query(models.FuelData)
        .filter(
            models.FuelData.station_id == record.station_id,
            models.FuelData.fuel_type == record.fuel_type,
            models.FuelData.timestamp < record.timestamp,
        )
        .order_by(models.FuelData.timestamp.desc())
        .first()
    )

    if previous is None:
        return 0  # first reading ever for this tank — nothing to compare against

    increase = record.stock_liters - previous.stock_liters
    if increase < RESTOCK_MIN_INCREASE_LITERS:
        return 0

    pct_of_capacity = increase / record.capacity_liters if record.capacity_liters else 0
    _append_alert(
        db,
        record.station_id,
        record.fuel_type,
        "RESTOCK",
        "info",
        f"Delivery received: stock rose by {increase:.0f}L "
        f"({pct_of_capacity:.0%} of capacity) to {record.stock_liters:.0f}L",
    )
    return 1


def generate_alerts_from_record(db :Session ,record: FuelDataResponse) -> int:
    generated = 0

    generated += _check_restock(db, record)

    if record.capacity_liters > 0:
        stock_pct = record.stock_liters / record.capacity_liters
        if stock_pct < 0.05:
            _append_alert(db,
                record.station_id,
                record.fuel_type,
                "LOW_STOCK",
                "critical",
                f"CRITICAL: Stock at {stock_pct:.1%} of capacity ({record.stock_liters:.0f}L remaining)",
            )
            generated += 1
        elif stock_pct < 0.15:
            _append_alert(db,
                record.station_id,
                record.fuel_type,
                "LOW_STOCK",
                "warning",
                f"Stock at {stock_pct:.1%} of capacity ({record.stock_liters:.0f}L remaining)",
            )
            generated += 1

    if record.official_price_tnd > 0:
        deviation_pct = abs(record.price_tnd - record.official_price_tnd) / record.official_price_tnd
        if deviation_pct > 0.10:
            _append_alert(db,
                record.station_id,
                record.fuel_type,
                "PRICE_ANOMALY",
                "critical",
                f"Price deviation of {deviation_pct:.1%} from official price ({record.price_tnd:.3f} vs {record.official_price_tnd:.3f} TND)",
            )
            generated += 1
        elif deviation_pct > 0.05:
            _append_alert(db,
                record.station_id,
                record.fuel_type,
                "PRICE_ANOMALY",
                "warning",
                f"Price deviation of {deviation_pct:.1%} from official price ({record.price_tnd:.3f} vs {record.official_price_tnd:.3f} TND)",
            )
            generated += 1

    if record.sales_last_5min_liters > 200:
        _append_alert(db,
            record.station_id,
            record.fuel_type,
            "HIGH_CONSUMPTION",
            "warning",
            f"Unusually high sales: {record.sales_last_5min_liters:.0f}L in last 5 minutes",
        )
        generated += 1

    if record.stock_liters == 0:
        _append_alert(db,
            record.station_id,
            record.fuel_type,
            "STATION_CRITICAL",
            "critical",
            f"Station is OUT OF STOCK for {record.fuel_type}",
        )
        generated += 1

    return generated
