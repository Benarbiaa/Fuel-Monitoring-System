"""
Ingestion consumer (ingestion_consumer/main.py): how one message is handled
(process_message) and how the loop retries, commits and keeps ordering
(consume_loop). See docs/debugging/kafka-reliability-review.md for the bugs
these tests guard against.
"""

from sqlalchemy.exc import OperationalError

import ingestion_consumer.main as consumer
from backend.services import storage
from tests.conftest import (
    FakeConsumer, FakeDLQProducer, FakeMessage, reading, stored_alerts, stored_readings,
)


def db_down(*args, **kwargs):
    raise OperationalError("INSERT ...", {}, Exception("connection refused"))


# --- process_message: one message --------------------------------------------

def test_valid_reading_is_stored_with_its_alerts(db):
    dlq = FakeDLQProducer()
    assert consumer.process_message(FakeMessage(reading(stock=0)), dlq) is True
    assert len(stored_readings(db)) == 1
    assert len(stored_alerts(db)) == 2   # LOW_STOCK + STATION_CRITICAL
    assert dlq.delivered == []


def test_redelivered_reading_is_stored_once(db):
    msg = FakeMessage(reading(stock=0))
    consumer.process_message(msg, FakeDLQProducer())
    assert consumer.process_message(msg, FakeDLQProducer()) is True
    assert len(stored_readings(db)) == 1
    assert len(stored_alerts(db)) == 2


def test_invalid_json_goes_to_dlq(db):
    dlq = FakeDLQProducer()
    assert consumer.process_message(FakeMessage(raw=b"not valid json"), dlq) is True
    [(topic, _, payload)] = dlq.delivered
    assert topic == consumer.DLQ_TOPIC
    assert payload["original_value"] == "not valid json"
    assert stored_readings(db) == []


def test_reading_missing_a_field_goes_to_dlq(db):
    payload = reading()
    del payload["stock_liters"]
    dlq = FakeDLQProducer()
    assert consumer.process_message(FakeMessage(payload), dlq) is True
    assert "stock_liters" in dlq.delivered[0][2]["error"]


def test_failed_dlq_write_is_not_committed(db):
    # Committing would lose the message from both topics (step C2).
    dlq = FakeDLQProducer(fail_with="broker unreachable")
    assert consumer.process_message(FakeMessage(raw=b"not valid json"), dlq) is False


def test_crash_between_alerts_rolls_back_the_reading(db, monkeypatch):
    # Step A: a half-processed message must leave nothing behind, otherwise
    # the retry is skipped as a duplicate and the missing alerts are lost.
    real_alerts = consumer.generate_alerts_from_record

    def crash_after_first_alert(session, record):
        storage.create_alert(session, record.station_id, record.fuel_type,
                             "LOW_STOCK", "critical", "first alert")
        raise RuntimeError("crash between alerts")

    monkeypatch.setattr(consumer, "generate_alerts_from_record", crash_after_first_alert)
    assert consumer.process_message(FakeMessage(reading(stock=0)), FakeDLQProducer()) is False
    assert stored_readings(db) == []
    assert stored_alerts(db) == []

    # The retry succeeds and creates every alert.
    monkeypatch.setattr(consumer, "generate_alerts_from_record", real_alerts)
    assert consumer.process_message(FakeMessage(reading(stock=0)), FakeDLQProducer()) is True
    assert len(stored_alerts(db)) == 2


def test_database_down_is_retried_forever(db, monkeypatch):
    monkeypatch.setattr(storage, "store_fuel_data_idempotent", db_down)
    dlq = FakeDLQProducer()
    assert consumer.process_message(FakeMessage(reading()), dlq, attempt=99) is False
    assert dlq.delivered == []   # valid data never goes to the DLQ


def test_unexpected_error_goes_to_dlq_after_max_attempts(db, monkeypatch):
    def bug(*args, **kwargs):
        raise ValueError("bug")

    monkeypatch.setattr(consumer, "generate_alerts_from_record", bug)
    dlq = FakeDLQProducer()
    last = consumer.MAX_ATTEMPTS
    for attempt in range(1, last):
        assert consumer.process_message(FakeMessage(reading()), dlq, attempt) is False
    assert dlq.delivered == []
    assert consumer.process_message(FakeMessage(reading()), dlq, last) is True
    assert f"failed after {last} attempts" in dlq.delivered[0][2]["error"]


# --- consume_loop: retries, commits, ordering --------------------------------

def run(fake_consumer, dlq):
    consumer.consume_loop(fake_consumer, dlq, is_running=lambda: not fake_consumer.done())


def test_failed_message_is_retried_not_skipped(db, monkeypatch, no_backoff):
    # Step B: before the fix, reading #2's commit moved the offset past #1
    # and #1 was lost for good.
    real_store = storage.store_fuel_data_idempotent
    failures = {"left": 2}

    def flaky_store(session, data):
        if failures["left"]:
            failures["left"] -= 1
            db_down()
        return real_store(session, data)

    monkeypatch.setattr(storage, "store_fuel_data_idempotent", flaky_store)
    fake = FakeConsumer([
        reading(timestamp="2026-01-01T12:00:00Z", stock=5000),
        reading(timestamp="2026-01-01T12:10:00Z", stock=4900),
    ])
    run(fake, FakeDLQProducer())

    assert fake.polled_offsets == [0, 0, 0, 1]   # #1 tried 3 times, then #2
    assert fake.committed == [1, 2]
    assert [r.stock_liters for r in stored_readings(db)] == [5000, 4900]   # in order


def test_poison_message_is_parked_and_the_rest_flows(db, monkeypatch, no_backoff):
    real_alerts = consumer.generate_alerts_from_record

    def bug_on_first_reading(session, record):
        if record.stock_liters == 5000:
            raise ValueError("bug")
        return real_alerts(session, record)

    monkeypatch.setattr(consumer, "generate_alerts_from_record", bug_on_first_reading)
    fake = FakeConsumer([reading(stock=5000), reading(timestamp="2026-01-01T12:10:00Z", stock=4900)])
    dlq = FakeDLQProducer()
    run(fake, dlq)

    assert fake.polled_offsets == [0] * consumer.MAX_ATTEMPTS + [1]
    assert fake.committed == [1, 2]
    assert len(dlq.delivered) == 1
    assert [r.stock_liters for r in stored_readings(db)] == [4900]


def test_message_waits_until_the_dlq_accepts_it(db, no_backoff):
    fake = FakeConsumer([b"not valid json", reading()])
    dlq = FakeDLQProducer(fail_with="broker unreachable", fail_times=2)
    run(fake, dlq)

    assert fake.polled_offsets == [0, 0, 0, 1]
    assert fake.committed == [1, 2]
    assert len(dlq.delivered) == 1   # exactly once, despite the retries
