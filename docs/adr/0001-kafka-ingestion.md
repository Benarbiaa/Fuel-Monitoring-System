# 1. Move fuel-reading ingestion from synchronous HTTP to Kafka

Status: Accepted, implemented (migration complete; reliability hardened 2026-10-07)
Date: 2026-09-17

## Context

Station readings were originally ingested via a synchronous `POST /ingest`
request that wrote directly to the database and generated alerts inline,
in the same request/response cycle. This has several limitations:

- Ingestion throughput is coupled to database write throughput and to
  whatever else the API process is doing at that moment.
- A slow or unavailable database makes ingestion itself fail; there's no
  buffer between "a station has data" and "the data is durably stored."
- There is no way to replay historical ingestion traffic — once a request
  is processed, it's gone. This matters for reprocessing after a bug fix
  (e.g. the RESTOCK alert-detection logic) or for feeding a forecasting
  model with realistic backfilled load.
- Every future consumer of this data (alerting, storage, future analytics)
  would need to be bolted onto the same request path, coupling unrelated
  concerns.

## Decision

Ingestion moves to a Kafka-based pipeline:

- Station simulators (and eventually `POST /ingest` itself) become
  **producers**, publishing readings to a topic instead of writing to the
  database directly.
- A dedicated **consumer service** owns all database writes and alert
  generation, decoupled from however data arrives.
- Messages are keyed by `station_id:fuel_type` so that all readings for a
  given tank are strictly ordered within a single partition — required by
  logic that compares a reading to the *previous* reading for that tank
  (e.g. restock detection).

### Topics

| Topic | Partitions | Replication | Purpose |
|---|---|---|---|
| `fuel.readings.raw` | 3 | 1 | Primary event stream of ingested readings, as received, before any validation/processing. Retained so it can be replayed. |
| `fuel.readings.dlq` | 1 | 1 | Dead-letter topic for messages that fail validation or repeatedly fail to process, so bad data is inspectable rather than silently dropped or crash-looping the consumer. |

### Local development environment

Kafka runs as a **single Docker container** (`apache/kafka:3.9.0`, KRaft
mode, no ZooKeeper), started via `scripts/start-kafka.sh`. All other
services (backend, chat, consumer, simulator, frontend) run natively on
the host, not in containers — Kafka is the one piece of infrastructure
that's genuinely painful to install/manage manually, while the app's own
services benefit from native hot-reload during development.

## Alternatives considered

- **RabbitMQ / Celery** — good fit for task queues, but messages are
  typically removed once acknowledged. We specifically want a retained,
  replayable log, which is Kafka's core model, not a queue's.
- **Redpanda** — Kafka-protocol-compatible, lighter to run locally.
  Rejected in favor of real Kafka for this project specifically because
  familiarity with actual Kafka operational quirks (see Consequences) is
  itself part of the learning goal.
- **Keep synchronous HTTP, add a queue only for alerts** — would solve
  less; the database-write coupling and lack of replay would remain for
  the primary ingestion path.

## Consequences

**Positive:**
- Ingestion and storage are decoupled; either can be scaled or restarted
  independently.
- Replayable history enables reprocessing after logic bugs and easy
  backfill for load testing / forecasting data.
- Clean seam for adding independent consumers later (e.g. splitting alert
  generation into its own consumer group reading the same topic).

**Negative / accepted trade-offs:**
- More moving parts for local development (a broker process to manage).
- At-least-once delivery semantics mean the consumer must be idempotent
  (unique constraint on `station_id` + `fuel_type` + `timestamp`, upsert
  on write) rather than relying on Kafka to prevent duplicates.
- Running Kafka via a plain `docker run` rather than as a fully installed
  local service means Docker itself becomes a hard local dependency.

**Known gotcha — single-broker replication factor:**
Kafka's internal topics (`__consumer_offsets`, `__transaction_state`)
default to `replication.factor = 3`. With a single broker, that default
can't be satisfied: the internal topic fails to be created (or ends up
under-replicated with no usable leader), and **any consumer-group-based
consumption then silently returns no messages**, with no clear
client-side error. This is invisible when testing with an explicit
partition+offset consumer (bypasses group coordination entirely), which
is what makes it confusing to debug — direct reads work, group-based
reads don't.

Fix: for a single-broker dev cluster, the broker must be started with:

```
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
```

These are baked into `scripts/start-kafka.sh`. This setting is
**dev-only** — a real multi-broker cluster should use the default of 3
(or at least 2) for durability of consumer offsets and transactional
state.

## Update (2026-10-07): delivery guarantees as implemented

A review of failure handling after the migration found that several of the
guarantees above held only on the happy path: a failed message could be
skipped rather than retried, a crash could keep a reading but lose its
alerts, and `/ingest` could answer 202 for a reading Kafka never received.
The full investigation, with reproductions, is in
[`docs/debugging/kafka-reliability-review.md`](../debugging/kafka-reliability-review.md).
The resulting design:

- **Producers** are idempotent (`enable.idempotence`), so retries can't
  reorder or duplicate readings within a partition. `/ingest` waits for
  the broker's confirmation before answering 202, and answers 503 if it
  doesn't get one.
- **The consumer** stores a reading and its alerts in one database
  transaction, and commits the Kafka offset only after that transaction
  commits (or after the message is confirmed in the DLQ).
- **Failed messages are retried in place**: the consumer seeks back to the
  failed offset and waits with exponential backoff, preserving per-tank
  ordering. Database outages are retried indefinitely; any other error is
  retried 5 times, then the message goes to `fuel.readings.dlq` with the
  error, so one poison message can't block its partition.
- **The broker address** is configurable with `KAFKA_BOOTSTRAP_SERVERS`
  (default `localhost:9092`).

Delivery is therefore at-least-once end to end, made safe by the
idempotent consumer (the unique constraint on `station_id` + `fuel_type` +
`timestamp`).
