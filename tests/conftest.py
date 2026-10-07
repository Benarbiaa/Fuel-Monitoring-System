"""
Shared fixtures for the ingestion pipeline tests.

The database is an in-memory SQLite one, created fresh for every test, so
tests need neither Postgres nor network access. Kafka is replaced by small
fakes (FakeMessage, FakeDLQProducer, FakeConsumer) that mimic only the
behavior the code relies on — except in test_kafka_integration.py, which
uses a real broker.
"""

import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import ingestion_consumer.main as consumer
from backend.database import models
from backend.database.database import Base

STATION = "TEST01"


def reading(timestamp="2026-01-01T12:00:00Z", stock=5000.0, price=2.55,
            official_price=2.55, sales=10.0, station=STATION, fuel_type="Gasoil50"):
    """A valid reading payload; override only what a test cares about."""
    return {
        "timestamp": timestamp,
        "station_id": station,
        "fuel_type": fuel_type,
        "price_tnd": price,
        "official_price_tnd": official_price,
        "stock_liters": stock,
        "capacity_liters": 10000.0,
        "sales_last_5min_liters": sales,
    }


class FakeMessage:
    """Stands in for confluent_kafka.Message."""

    def __init__(self, payload=None, raw=None, offset=0, partition=0, topic="fuel.readings.raw"):
        self._value = raw if raw is not None else json.dumps(payload).encode()
        self._offset = offset
        self._partition = partition
        self._topic = topic

    def value(self):
        return self._value

    def key(self):
        return b"TEST01:Gasoil50"

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return None


class FakeDLQProducer:
    """
    Stands in for the DLQ Producer. Messages are 'delivered' on flush(),
    like the real one: each call's callback fires with `fail_with` as the
    error (None means success). Set fail_times to fail only the first N.
    """

    def __init__(self, fail_with=None, fail_times=None):
        self.fail_with = fail_with
        self.fail_times = fail_times
        self.delivered = []
        self._pending = []

    def produce(self, topic, key, value, callback):
        self._pending.append((topic, key, value, callback))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        for topic, key, value, callback in self._pending:
            failing = self.fail_with is not None and (self.fail_times is None or self.fail_times > 0)
            if failing:
                if self.fail_times is not None:
                    self.fail_times -= 1
                callback(self.fail_with, None)
            else:
                self.delivered.append((topic, key, json.loads(value)))
                callback(None, None)
        self._pending = []
        return 0


class FakeConsumer:
    """
    One-partition stand-in for confluent_kafka.Consumer, with real offset
    semantics: poll() returns the message at the current position and moves
    past it, seek() moves the position, commit(msg) records msg.offset()+1
    ("next to read"), exactly like Kafka's committed offset.
    """

    def __init__(self, payloads):
        self.messages = [
            FakeMessage(p, offset=i) if isinstance(p, dict) else FakeMessage(raw=p, offset=i)
            for i, p in enumerate(payloads)
        ]
        self.position = 0
        self.committed = []
        self.polled_offsets = []

    def poll(self, timeout=None):
        if len(self.polled_offsets) > 200:
            raise RuntimeError("consume loop doesn't seem to terminate")
        if self.position >= len(self.messages):
            return None
        msg = self.messages[self.position]
        self.position += 1
        self.polled_offsets.append(msg.offset())
        return msg

    def seek(self, topic_partition):
        self.position = topic_partition.offset

    def commit(self, msg):
        self.committed.append(msg.offset() + 1)

    def done(self):
        return self.position >= len(self.messages)


@pytest.fixture
def session_factory(monkeypatch):
    """Fresh in-memory database; the consumer is pointed at it."""
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        # One shared connection, otherwise every new connection would get
        # its own, empty in-memory database.
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    monkeypatch.setattr(consumer, "SessionLocal", factory)
    yield factory
    engine.dispose()


@pytest.fixture
def db(session_factory):
    session = session_factory()
    yield session
    session.close()


@pytest.fixture
def no_backoff(monkeypatch):
    """Retries happen immediately instead of waiting 1s, 2s, 4s..."""
    monkeypatch.setattr(consumer, "_backoff_seconds", lambda attempt: 0)


def stored_readings(db):
    db.expire_all()
    return db.query(models.FuelData).order_by(models.FuelData.id).all()


def stored_alerts(db):
    db.expire_all()
    return db.query(models.Alert).order_by(models.Alert.id).all()
