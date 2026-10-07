"""
ingestion_consumer/main.py
---------------------------
Standalone service that consumes fuel readings from fuel.readings.raw and
is the sole writer to the database (Phase 2 of the Kafka ingestion
migration — see docs/adr/0001-kafka-ingestion.md).

Run from the project root, so `backend` is importable as a package:
    python -m ingestion_consumer.main
"""

from __future__ import annotations

import json
import logging
import signal
import time

from confluent_kafka import Consumer, Producer, KafkaError, TopicPartition
from pydantic import ValidationError
from sqlalchemy.exc import OperationalError

from backend.database.database import SessionLocal
from backend.schemas import FuelData as FuelDataSchema
from backend.services import storage
from backend.routes.data import generate_alerts_from_record

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ingestion_consumer")

BOOTSTRAP_SERVERS = "localhost:9092"
READINGS_TOPIC = "fuel.readings.raw"
DLQ_TOPIC = "fuel.readings.dlq"

# Fixed, permanent group ID. This is what lets Kafka remember "how far has
# the ingestion consumer read" across restarts — a random or changing ID
# here would make every restart look like a brand-new consumer with no
# history, defeating the point of committed offsets.
GROUP_ID = "ingestion-consumer"

# Retry policy for messages that fail to process (see process_message).
# Transient database errors are retried forever — the data is valid and
# must not be lost. Any other error is retried MAX_ATTEMPTS times, then the
# message is treated as a poison message and routed to the DLQ, so one bad
# message can't block every later reading on its partition forever.
MAX_ATTEMPTS = 5
# Waits between retries double each time (1s, 2s, 4s, ...) up to this cap.
# Kept well under max.poll.interval.ms (5 min by default): if poll() isn't
# called within that window, Kafka assumes this consumer is dead and
# removes it from the group.
BACKOFF_MAX_SECONDS = 30


def _send_to_dlq(dlq_producer: Producer, key, raw_value: bytes, error: str) -> None:
    """
    Publishes an unprocessable message to the dead-letter topic, wrapped
    with the error that made it unprocessable, so it's inspectable later
    instead of just vanishing into a log line no one will read.
    """
    dlq_payload = {
        "error": error,
        "original_value": raw_value.decode("utf-8", errors="replace"),
    }
    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=key,
        value=json.dumps(dlq_payload).encode("utf-8"),
    )
    dlq_producer.poll(0)


def process_message(msg, dlq_producer: Producer, attempt: int = 1) -> bool:
    """
    Handles one Kafka message end to end: validate, write, generate alerts.

    `attempt` is 1 the first time a message is seen and goes up by one on
    every retry of that same message (see main()).

    Returns True if this message's offset should be committed — meaning
    "we're done with this message, move on" — which covers three outcomes:
    it was processed successfully, it was permanently invalid and has been
    routed to the DLQ, or it kept failing for MAX_ATTEMPTS and has been
    routed to the DLQ. Returns False when it should be retried.
    """
    raw_value = msg.value()
    key = msg.key()

    # 1. Deserialize + validate first. A malformed or schema-invalid
    # message will NEVER succeed no matter how many times it's retried,
    # so this failure mode goes straight to the DLQ rather than being
    # left uncommitted (which would just loop forever on every restart).
    try:
        payload_dict = json.loads(raw_value.decode("utf-8"))
        validated = FuelDataSchema(**payload_dict)
    except (json.JSONDecodeError, ValidationError) as e:
        logger.warning("Invalid message, routing to DLQ: %s", e)
        _send_to_dlq(dlq_producer, key, raw_value, error=str(e))
        return True  # permanently handled — commit past it

    # 2. Write the reading (idempotently — see storage.py) and generate its
    # alerts in ONE transaction: the helpers only flush, and the single
    # commit below makes everything permanent at once. If anything fails
    # before it, the rollback undoes the reading too, so a retry can never
    # mistake a half-processed message for an already-processed one.
    db = SessionLocal()
    try:
        record = storage.store_fuel_data_idempotent(db, validated)
        if record is None:
            # This exact reading was already stored by an earlier attempt
            # (a redelivered message). Its alerts were already generated
            # the first time it was processed — nothing new to do here.
            logger.info(
                "Duplicate reading skipped: %s/%s @ %s",
                validated.station_id, validated.fuel_type, validated.timestamp,
            )
            return True

        generate_alerts_from_record(db, record)
        db.commit()
        logger.info(
            "Stored reading: %s/%s stock=%.1fL",
            record.station_id, record.fuel_type, record.stock_liters,
        )
        return True

    except OperationalError as e:
        # The database is unreachable or the connection dropped. The
        # message itself is fine, so retry it for as long as it takes.
        logger.error("Database unavailable (attempt %d), will retry: %s", attempt, e)
        db.rollback()
        return False

    except Exception as e:
        # Unexpected failure (e.g. a bug in alert generation, or a value the
        # database rejects). It might be a one-off, so retry a few times;
        # if it keeps failing, it's a poison message: park it in the DLQ
        # and move on instead of blocking the partition forever.
        db.rollback()
        if attempt >= MAX_ATTEMPTS:
            logger.error("Failed %d times, routing to DLQ: %s", attempt, e)
            _send_to_dlq(dlq_producer, key, raw_value,
                         error=f"failed after {attempt} attempts: {e}")
            return True
        logger.error("Processing failed (attempt %d/%d), will retry: %s",
                     attempt, MAX_ATTEMPTS, e)
        return False

    finally:
        db.close()


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        # Manual commits only, tied to a message actually being fully
        # handled (see process_message) — never on a timer. This is what
        # prevents a crash from silently skipping data: auto-commit could
        # acknowledge a message before we've actually written it.
        "enable.auto.commit": False,
        # Where to start if this consumer group has never committed an
        # offset before (its very first run ever). "earliest" means a
        # brand-new group replays the topic's full retained history,
        # rather than only seeing messages produced after it started —
        # important since fuel.readings.raw already has data in it from
        # the simulator running during Phase 1 testing.
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([READINGS_TOPIC])

    dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    running = True

    def _handle_shutdown(signum, frame):
        nonlocal running
        logger.info("Shutdown requested, finishing current message then exiting...")
        running = False

    # SIGINT (Ctrl+C) and SIGTERM (e.g. `kill`, or a process manager
    # stopping the service) both trigger a graceful exit rather than an
    # abrupt kill mid-message.
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    logger.info("Ingestion consumer started. group=%s topic=%s", GROUP_ID, READINGS_TOPIC)

    # Which try this is for the current message. A single counter is enough:
    # after a failure we seek back, so the next message poll() returns is
    # always the one being retried.
    attempt = 1

    try:
        while running:
            # Blocks up to 1s waiting for a message; returns None if
            # nothing arrived in that window, so the loop can check
            # `running` regularly instead of blocking forever.
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka error: %s", msg.error())
                continue

            should_commit = process_message(msg, dlq_producer, attempt)
            if should_commit:
                consumer.commit(msg)
                attempt = 1
                continue

            # Retry in place. Not committing is not enough on its own: the
            # consumer's in-memory position has already moved past this
            # message, and committing any LATER message on this partition
            # would mark this one as done too (a committed offset means
            # "everything before here is handled"). Seeking back makes the
            # next poll() return this same message, so later readings for
            # the same tank can't overtake it.
            consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
            delay = min(2 ** (attempt - 1), BACKOFF_MAX_SECONDS)
            attempt += 1
            # Sleep in small steps so Ctrl+C / SIGTERM still exits promptly.
            deadline = time.monotonic() + delay
            while running and time.monotonic() < deadline:
                time.sleep(0.2)

    finally:
        logger.info("Closing consumer, flushing DLQ producer...")
        dlq_producer.flush(10.0)
        consumer.close()


if __name__ == "__main__":
    main()