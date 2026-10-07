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

from confluent_kafka import Consumer, Producer, KafkaError
from pydantic import ValidationError

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


def process_message(msg, dlq_producer: Producer) -> bool:
    """
    Handles one Kafka message end to end: validate, write, generate alerts.

    Returns True if this message's offset should be committed — meaning
    "we're done with this message, move on" — which covers two distinct
    outcomes: it was processed successfully, OR it was permanently invalid
    and has been routed to the DLQ. Returns False only for transient
    failures (e.g. the database is briefly unreachable), meaning "leave
    this uncommitted so it gets redelivered and retried later."
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

    except Exception as e:
        # Anything else — most plausibly a transient DB outage — is NOT
        # committed. The message will be redelivered and retried the next
        # time this consumer (re)starts and rejoins the group.
        logger.error("Transient failure, will retry on restart: %s", e)
        db.rollback()
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

            should_commit = process_message(msg, dlq_producer)
            if should_commit:
                consumer.commit(msg)

    finally:
        logger.info("Closing consumer, flushing DLQ producer...")
        dlq_producer.flush(10.0)
        consumer.close()


if __name__ == "__main__":
    main()