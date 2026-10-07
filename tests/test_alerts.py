"""Alert rules applied to each incoming reading (backend/services/alerts.py)."""

import pytest

from backend.schemas import FuelData
from backend.services import storage
from backend.services.alerts import generate_alerts_from_record
from tests.conftest import reading, stored_alerts


def store_and_alert(db, **overrides):
    record = storage.store_fuel_data_idempotent(db, FuelData(**reading(**overrides)))
    generate_alerts_from_record(db, record)
    db.commit()
    return [(a.alert_type, a.severity) for a in stored_alerts(db)]


def test_normal_reading_creates_no_alert(db):
    assert store_and_alert(db) == []


@pytest.mark.parametrize("overrides, expected", [
    ({"stock": 1000}, [("LOW_STOCK", "warning")]),                      # 10% of capacity
    ({"stock": 300}, [("LOW_STOCK", "critical")]),                      # 3%
    ({"stock": 0}, [("LOW_STOCK", "critical"), ("STATION_CRITICAL", "critical")]),
    ({"price": 2.75}, [("PRICE_ANOMALY", "warning")]),                  # +7.8%
    ({"price": 2.85}, [("PRICE_ANOMALY", "critical")]),                 # +11.8%
    ({"sales": 250}, [("HIGH_CONSUMPTION", "warning")]),
])
def test_threshold_alerts(db, overrides, expected):
    assert store_and_alert(db, **overrides) == expected


def test_stock_jump_between_readings_is_a_restock(db):
    store_and_alert(db, timestamp="2026-01-01T12:00:00Z", stock=2000)
    alerts = store_and_alert(db, timestamp="2026-01-01T12:10:00Z", stock=8000)
    assert ("RESTOCK", "info") in alerts


def test_small_stock_increase_is_not_a_restock(db):
    store_and_alert(db, timestamp="2026-01-01T12:00:00Z", stock=5000)
    alerts = store_and_alert(db, timestamp="2026-01-01T12:10:00Z", stock=5030)
    assert alerts == []
