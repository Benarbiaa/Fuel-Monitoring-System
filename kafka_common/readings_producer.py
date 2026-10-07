"""
kafka_common/readings_producer.py
----------------------------------
Thin wrapper around confluent_kafka.Producer for publishing fuel readings
to fuel.readings.raw. Shared by every producer in the system — the station
simulator (stations/agilAgentStation.py) and the backend's /ingest endpoint
(backend/routes/ingest.py) — so both use the exact same keying scheme and
producer configuration. This matters concretely: if the two producers keyed
messages differently, HTTP-sourced and simulator-sourced readings for the
same tank could land on different partitions, breaking the per-tank
ordering guarantee that restock detection depends on.

Design notes (see docs/adr/0001-kafka-ingestion.md for the full rationale):

- Messages are keyed by "{station_id}:{fuel_type}" so that Kafka's
  per-partition ordering guarantee applies per-tank. This matters because
  the backend's restock-detection logic (backend/routes/data.py) compares
  each reading to the immediately preceding reading for the same tank —
  if two readings for one tank could land in different partitions and be
  consumed out of order, that comparison would be meaningless.
- acks="all" is used even though our dev cluster has replication factor 1
  (so there's only one replica to acknowledge anyway). This keeps the
  producer config identical to what a real multi-broker cluster would use,
  so nothing needs to change when this moves off a single-node dev setup.
- The producer is idempotent (enable.idempotence). Without it, retries can
  break the per-tank ordering the message key exists for: the producer
  sends several batches without waiting for each acknowledgement, so if
  batch 1 fails transiently and batch 2 succeeds, the retried batch 1 lands
  AFTER batch 2. A retry can also write a message twice when the write
  succeeded but its acknowledgement was lost. With idempotence the broker
  gives this producer an ID and tracks a sequence number per partition: it
  drops duplicates and refuses out-of-order writes, so retries are safe.
  (The Java client enables this by default since Kafka 3.0; librdkafka,
  which confluent-kafka uses, does not.)
- Delivery is asynchronous (produce() returns immediately); a callback
  logs success/failure. flush() is called on shutdown to make sure nothing
  is lost when the process exits.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

from confluent_kafka import Producer

logger = logging.getLogger("kafka_producer")

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
READINGS_TOPIC = "fuel.readings.raw"


def _delivery_report(err, msg):
    """
    Called once per message, asynchronously, once Kafka has accepted or
    rejected it. This is where produce-side failures actually surface —
    produce() itself never raises for broker-side problems, only for
    local, immediate errors (e.g. the local queue being full).
    """
    if err is not None:
        logger.error(
            "Delivery failed for key=%s: %s",
            msg.key().decode() if msg.key() else None,
            err,
        )
    else:
        logger.debug(
            "Delivered to %s [partition %d] @ offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


class ReadingsProducer:
    """
    Publishes fuel-reading payloads to the fuel.readings.raw topic.

    Usage:
        producer = ReadingsProducer()
        producer.send(station_id="BI00001", fuel_type="Gasoil50", payload={...})
        ...
        producer.close()  # flush on shutdown
    """

    def __init__(self, bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS):
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                # Safe retries: no duplicates, no reordering within a
                # partition (see module docstring).
                "enable.idempotence": True,
                # Retry transient errors (e.g. broker briefly unreachable)
                # rather than failing the first blip. linger.ms batches
                # near-simultaneous sends slightly for efficiency without
                # meaningfully delaying a 10-minute ingestion cadence.
                "retries": 5,
                "retry.backoff.ms": 500,
                "linger.ms": 50,
            }
        )

    def send(self, station_id: str, fuel_type: str, payload: dict[str, Any]) -> None:
        """
        Publishes one reading. Non-blocking: queues the message and returns;
        delivery success/failure is reported later via the callback. Call
        poll(0) here to let queued delivery-report callbacks fire without
        blocking the caller.
        """
        key = f"{station_id}:{fuel_type}".encode("utf-8")
        value = json.dumps(payload).encode("utf-8")
        try:
            self._producer.produce(
                topic=READINGS_TOPIC,
                key=key,
                value=value,
                callback=_delivery_report,
            )
        except BufferError:
            # Local producer queue is full — the broker isn't keeping up
            # (or is unreachable) and delivery reports haven't cleared
            # queued messages yet. Block briefly to drain it rather than
            # dropping this reading.
            logger.warning("Producer queue full, blocking briefly to drain")
            self._producer.poll(1.0)
            self._producer.produce(
                topic=READINGS_TOPIC,
                key=key,
                value=value,
                callback=_delivery_report,
            )

        # Non-blocking poll to service any pending delivery-report callbacks.
        self._producer.poll(0)

    def close(self, timeout: Optional[float] = 10.0) -> None:
        """Flushes any in-flight messages before shutdown. Call this once,
        on process exit, so a Ctrl+C doesn't silently drop the last batch."""
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning(
                "%d message(s) still undelivered after flush timeout", remaining
            )