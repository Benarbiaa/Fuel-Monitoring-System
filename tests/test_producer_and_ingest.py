"""
The shared readings producer (kafka_common/readings_producer.py) and the
POST /ingest endpoint that uses it (backend/routes/ingest.py).
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import kafka_common.readings_producer as rp
from backend.routes import ingest
from tests.conftest import reading


class FakeKafkaProducer:
    """
    Stands in for confluent_kafka.Producer inside ReadingsProducer. The
    delivery callback fires on poll(): with `error` (None = success), or
    never if confirm=False (a broker that never answers).
    """

    def __init__(self, error=None, confirm=True, queue_full=False):
        self.error, self.confirm, self.queue_full = error, confirm, queue_full
        self.produced = []
        self._callbacks = []

    def produce(self, topic, key, value, callback):
        if self.queue_full:
            raise BufferError("Local: Queue full")
        self.produced.append((topic, key))
        self._callbacks.append(callback)

    def poll(self, timeout=0):
        if self.confirm:
            for cb in self._callbacks:
                cb(self.error, FakeDeliveredMessage())
            self._callbacks = []
        return 0

    def __len__(self):
        return len(self._callbacks)


class FakeDeliveredMessage:
    def key(self): return b"k"
    def topic(self): return "t"
    def partition(self): return 0
    def offset(self): return 0


def producer_with(fake):
    p = rp.ReadingsProducer.__new__(rp.ReadingsProducer)   # skip the real client
    p._producer = fake
    return p


# --- ReadingsProducer --------------------------------------------------------

def test_producer_is_idempotent(monkeypatch):
    configs = []
    monkeypatch.setattr(rp, "Producer", lambda conf: configs.append(conf))
    rp.ReadingsProducer()
    assert configs[0]["enable.idempotence"] is True
    assert configs[0]["acks"] == "all"


def test_bootstrap_servers_come_from_the_environment(monkeypatch):
    configs = []
    monkeypatch.setattr(rp, "Producer", lambda conf: configs.append(conf))
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    rp.ReadingsProducer()
    assert configs[0]["bootstrap.servers"] == "kafka:9092"


def test_messages_are_keyed_per_tank():
    fake = FakeKafkaProducer()
    producer_with(fake).send_and_wait("BI00001", "Gasoil50", {"x": 1})
    assert fake.produced == [(rp.READINGS_TOPIC, b"BI00001:Gasoil50")]


def test_send_and_wait_raises_when_kafka_rejects():
    with pytest.raises(rp.DeliveryError):
        producer_with(FakeKafkaProducer(error="broker error")).send_and_wait("S", "F", {}, timeout=1)


def test_send_and_wait_raises_when_kafka_never_answers():
    with pytest.raises(rp.DeliveryError, match="not confirmed"):
        producer_with(FakeKafkaProducer(confirm=False)).send_and_wait("S", "F", {}, timeout=0.3)


# --- POST /ingest ------------------------------------------------------------

def post_reading(fake):
    # Only the ingest router: the real app's startup would also start the
    # alert watcher (which calls the LLM API) and a real Kafka producer.
    app = FastAPI()
    app.include_router(ingest.router, prefix="/ingest")
    app.state.kafka_producer = producer_with(fake)
    return TestClient(app).post("/ingest/", json=reading())


def test_ingest_returns_202_once_kafka_confirms():
    r = post_reading(FakeKafkaProducer())
    assert r.status_code == 202
    assert "id" not in r.json()   # queued in Kafka, not yet in the database


@pytest.mark.parametrize("fake", [
    FakeKafkaProducer(error="broker error"),
    FakeKafkaProducer(queue_full=True),
], ids=["kafka-rejects", "queue-full"])
def test_ingest_returns_503_when_kafka_does_not_have_the_reading(fake):
    assert post_reading(fake).status_code == 503


def test_ingest_returns_503_when_kafka_never_answers(monkeypatch):
    monkeypatch.setattr(ingest, "KAFKA_CONFIRM_TIMEOUT_SECONDS", 0.3)
    assert post_reading(FakeKafkaProducer(confirm=False)).status_code == 503
