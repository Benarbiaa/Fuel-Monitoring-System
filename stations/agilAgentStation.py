import argparse
import sys
import threading
import time
import random
from datetime import datetime, timezone
from pathlib import Path

import requests

# kafka_common (and its confluent-kafka dependency) is imported lazily in
# main(), only when --sink=kafka is actually used, so running in --sink=http
# mode never requires confluent-kafka to be installed.
#
# kafka_common/ lives at the project root, one level up from this file's
# own directory (stations/). Running this script directly only puts
# stations/ itself on sys.path, not the project root, so we add the root
# explicitly rather than relying on however this happens to be invoked.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

BASE_URL = "http://localhost:8000"

# How often each station pushes a new reading. The backend/alert thresholds
# (e.g. HIGH_CONSUMPTION = sales > 200L "in 5 minutes") were tuned for a
# 5-minute cadence, so sales volumes are scaled proportionally to whatever
# interval we actually run at, rather than hardcoding 5-minute numbers.
INTERVAL_SECONDS = 10 * 60  # 10 minutes
INTERVAL_MINUTES = INTERVAL_SECONDS / 60

# Per-station base config. Different locations/tank sizes/prices make the
# fleet feel like real, distinct stations rather than 3 clones.
STATION_CONFIGS = {
    "BI00001": {
        "tanks": {
            "Gasoil50": {"stock": 9000.0, "capacity": 10000.0, "price": 2.550},
            "SansPlomb": {"stock": 7000.0, "capacity": 12000.0, "price": 2.300},
        },
        # Busier city-centre station: higher baseline traffic.
        "traffic_scale": 1.3,
    },
    "BI00002": {
        "tanks": {
            "Gasoil50": {"stock": 8000.0, "capacity": 10000.0, "price": 2.550},
            "SansPlomb": {"stock": 6000.0, "capacity": 12000.0, "price": 2.300},
        },
        "traffic_scale": 1.0,
    },
    "BI00003": {
        "tanks": {
            "Gasoil50": {"stock": 5000.0, "capacity": 8000.0, "price": 2.560},
            "SansPlomb": {"stock": 4000.0, "capacity": 9000.0, "price": 2.310},
        },
        # Quieter coastal-town station: lower baseline traffic.
        "traffic_scale": 0.7,
    },
}

ANOMALY_FREQUENCY = 0.01        # chance per tank per tick of a fleet-refuel spike
PRICE_ANOMALY_FREQUENCY = 0.03  # chance per tank per tick of a price bump
RESTOCK_THRESHOLD = 0.20        # restock (delivery truck) once stock < 20% capacity
RESTOCK_CHANCE = 0.15           # per tick, once under threshold, chance a delivery arrives


def traffic_multiplier(hour: int) -> float:
    """
    Approximates a daily demand curve instead of a flat peak/off-peak flag:
    quiet overnight, ramps up for morning/evening rush, moderate through the
    midday, small dip late evening.
    """
    if 0 <= hour < 5:
        return 0.15
    if 5 <= hour < 7:
        return 0.6
    if 7 <= hour < 9:
        return 2.0          # morning rush
    if 9 <= hour < 12:
        return 1.0
    if 12 <= hour < 14:
        return 1.3          # lunch bump
    if 14 <= hour < 17:
        return 0.9
    if 17 <= hour < 19:
        return 2.2          # evening rush
    if 19 <= hour < 22:
        return 1.1
    return 0.4               # late evening wind-down


class StationAgent:
    def __init__(
        self,
        station_id: str,
        tanks: dict,
        traffic_scale: float = 1.0,
        sink: str = "kafka",
        kafka_producer=None,
    ):
        self.station_id = station_id
        self.traffic_scale = traffic_scale
        self.sink = sink
        self.kafka_producer = kafka_producer
        # Deep-copy so mutating one station's tanks never touches another's.
        self.tanks = {name: dict(cfg) for name, cfg in tanks.items()}

    def simulate_step(self):
        """Simulates one INTERVAL_SECONDS window of activity for this station."""
        hour = datetime.now(timezone.utc).hour
        mult = traffic_multiplier(hour) * self.traffic_scale

        for fuel_type, data in self.tanks.items():
            # 1. Random sales, scaled to the actual interval length (base
            # rates below are calibrated per 5 minutes, then scaled for a
            # 10-minute window).
            if random.random() < ANOMALY_FREQUENCY:
                sales = random.uniform(210, 250) * (INTERVAL_MINUTES / 5)
            else:
                sales = random.uniform(5, 30) * mult * (INTERVAL_MINUTES / 5)

            # Never sell more than what's actually in the tank.
            sales = min(sales, data["stock"])
            data["stock"] = max(0.0, data["stock"] - sales)

            # 2. Occasional delivery truck restock once stock runs low, so
            # tanks don't just monotonically drain to zero and stay there.
            if data["stock"] < data["capacity"] * RESTOCK_THRESHOLD and random.random() < RESTOCK_CHANCE:
                delivery = random.uniform(0.6, 0.95) * data["capacity"]
                data["stock"] = min(data["capacity"], data["stock"] + delivery)
                print(f"[{self.station_id}/{fuel_type}] \U0001f69a Delivery received: +{delivery:.0f}L")

            # 3. Occasional price anomaly (manager raises price above the
            # station's official/base price).
            if random.random() < PRICE_ANOMALY_FREQUENCY:
                data["price"] += random.uniform(0.15, 0.30)

            current_price = round(data["price"], 3)

            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "station_id": self.station_id,
                "fuel_type": fuel_type,
                "price_tnd": current_price,
                "official_price_tnd": data.get("official_price", data["price"]),
                "stock_liters": round(data["stock"], 2),
                "capacity_liters": data["capacity"],
                "sales_last_5min_liters": round(sales, 2),
            }

            self._send(payload, sales, current_price)

    def _send(self, payload: dict, sales: float, price: float):
        if self.sink == "kafka":
            self._send_kafka(payload, sales, price)
        else:
            self._send_http(payload, sales, price)

    def _send_kafka(self, payload: dict, sales: float, price: float):
        # Non-blocking: queues the message with the producer and returns.
        # Delivery success/failure is reported asynchronously by the
        # producer's callback (see kafka_producer.py), not here — so a
        # print here only confirms "handed to the producer," not "durably
        # stored," which is an intentionally different guarantee than the
        # old HTTP path gave.
        self.kafka_producer.send(
            station_id=self.station_id,
            fuel_type=payload["fuel_type"],
            payload=payload,
        )
        print(
            f"[{self.station_id}/{payload['fuel_type']}] "
            f"Queued: {sales:.1f}L sold | Stock: {payload['stock_liters']:.1f}L | "
            f"Price: {price:.3f} TND"
        )

    def _send_http(self, payload: dict, sales: float, price: float):
        try:
            response = requests.post(f"{BASE_URL}/ingest", json=payload, timeout=10)
            # /ingest answers 202 Accepted once Kafka has the reading (it is
            # stored by the ingestion consumer moments later), not 200/201.
            if response.status_code == 202:
                print(
                    f"[{self.station_id}/{payload['fuel_type']}] "
                    f"Sent: {sales:.1f}L sold | Stock: {payload['stock_liters']:.1f}L | "
                    f"Price: {price:.3f} TND"
                )
            else:
                print(f"\u274c [{self.station_id}] Error {response.status_code}: {response.text}")
        except Exception as e:
            print(f"\u274c [{self.station_id}] Connection failed: {e}")

    def run(self, interval: int = INTERVAL_SECONDS):
        print(f"Station Agent {self.station_id} is now online (every {interval // 60} min)...")
        while True:
            self.simulate_step()
            time.sleep(interval)


def _freeze_official_price(tanks: dict):
    """Store each tank's starting price as the 'official' price so anomalies
    can be measured against a stable baseline, matching how PRICE_ANOMALY
    alerts are defined (deviation from official price)."""
    for data in tanks.values():
        data["official_price"] = data["price"]


def _parse_args():
    parser = argparse.ArgumentParser(description="Fuel station telemetry simulator")
    parser.add_argument(
        "--sink",
        choices=["kafka", "http"],
        default="kafka",
        help=(
            "Where readings are sent. 'kafka' publishes to fuel.readings.raw "
            "(requires a running broker; set KAFKA_BOOTSTRAP_SERVERS if it "
            "isn't at localhost:9092). 'http' POSTs "
            "directly to /ingest, matching the original pre-Kafka behavior — "
            "kept as a fallback if Kafka isn't available."
        ),
    )
    return parser.parse_args()


def main():
    args = _parse_args()

    # Imported lazily and only in kafka mode, so `--sink http` never requires
    # confluent-kafka to be installed or a broker to be reachable.
    kafka_producer = None
    if args.sink == "kafka":
        from kafka_common.readings_producer import ReadingsProducer
        kafka_producer = ReadingsProducer()

    agents = []
    for station_id, cfg in STATION_CONFIGS.items():
        _freeze_official_price(cfg["tanks"])
        agents.append(
            StationAgent(
                station_id,
                cfg["tanks"],
                cfg["traffic_scale"],
                sink=args.sink,
                kafka_producer=kafka_producer,
            )
        )

    threads = [threading.Thread(target=agent.run, daemon=True) for agent in agents]

    # Stagger start times slightly so all 3 stations don't all hit /ingest
    # in the exact same instant every 10 minutes.
    for t in threads:
        t.start()
        time.sleep(2)

    print(f"\n{len(agents)} station agents running (sink={args.sink}). Press Ctrl+C to stop.\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping station agents...")
    finally:
        if kafka_producer is not None:
            print("Flushing any in-flight Kafka messages...")
            kafka_producer.close()


if __name__ == "__main__":
    main()