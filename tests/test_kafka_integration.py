"""
End to end against a real Kafka broker: producer → topic → consumer loop →
database, with the database failing at first. Skipped when no broker is
reachable (start one with ./scripts/start-kafka.sh).

Each run uses its own throwaway topic and consumer group, so it never
touches fuel.readings.raw or the real consumer's committed offsets.
"""

import os
import time
import uuid

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

import ingestion_consumer.main as consumer
import kafka_common.readings_producer as rp
from backend.services import storage
from tests.conftest import reading, stored_readings
from tests.test_consumer import db_down

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

pytestmark = pytest.mark.kafka


@pytest.fixture
def topic():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    try:
        admin.list_topics(timeout=3)
    except Exception:
        pytest.skip(f"no Kafka broker at {BOOTSTRAP}")
    name = f"test.readings.{uuid.uuid4().hex[:8]}"
    admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])[name].result(10)
    yield name
    admin.delete_topics([name])[name].result(10)   # wait, or the process may exit first


def test_readings_survive_a_database_outage_in_order(db, topic, monkeypatch, no_backoff):
    # Publish two readings for the same tank with the real producer.
    monkeypatch.setattr(rp, "READINGS_TOPIC", topic)
    producer = rp.ReadingsProducer(BOOTSTRAP)
    producer.send_and_wait("TEST01", "Gasoil50", reading(timestamp="2026-01-01T12:00:00Z", stock=5000))
    producer.send_and_wait("TEST01", "Gasoil50", reading(timestamp="2026-01-01T12:10:00Z", stock=4900))
    producer.close()

    # The database is down for the first two writes.
    real_store = storage.store_fuel_data_idempotent
    failures = {"left": 2}

    def flaky_store(session, data):
        if failures["left"]:
            failures["left"] -= 1
            db_down()
        return real_store(session, data)

    monkeypatch.setattr(storage, "store_fuel_data_idempotent", flaky_store)

    kafka_consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"test-{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    kafka_consumer.subscribe([topic])
    deadline = time.monotonic() + 30
    try:
        consumer.consume_loop(
            kafka_consumer, dlq_producer=None,
            is_running=lambda: len(stored_readings(db)) < 2 and time.monotonic() < deadline,
        )
    finally:
        kafka_consumer.close()

    assert failures["left"] == 0
    assert [r.stock_liters for r in stored_readings(db)] == [5000, 4900]
