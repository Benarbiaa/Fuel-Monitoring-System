# Kafka pipeline reliability review: what happens when something fails?

After the Kafka migration ([ADR 0001](../adr/0001-kafka-ingestion.md)), the
pipeline worked on the happy path: readings flowed from the simulator and
`POST /ingest` through `fuel.readings.raw` into Postgres, duplicates were
ignored and malformed messages landed in the DLQ. This review asked the
next question: **what happens to a reading when something fails along the
way** — the database drops, the broker is down, the consumer crashes, a
message can't be processed?

Every problem below was **reproduced against the real broker and database
before being fixed**, and the same reproduction was rerun after the fix.
That turned out to matter: in several places the code's own comments
described the intended behavior, and the actual behavior was different.

## Summary

| # | Problem | Impact | Fix (commit) |
|---|---|---|---|
| 1 | A reading and its alerts were saved in separate transactions | Crash mid-way → reading saved, retry skipped as "duplicate", **alerts lost** | One transaction per message (`e1f11ce`) |
| 2 | A failed message was never retried | **Reading lost for good** once a later message was committed | Seek back and retry with backoff; poison messages to the DLQ (`404748d`) |
| 3 | Producer retries without idempotence | Readings for one tank could be **reordered or duplicated** | `enable.idempotence` (`727b0c2`) |
| 4 | DLQ write not confirmed before committing | A bad message could vanish from **both** topics | Wait for the broker's confirmation (`727b0c2`) |
| 5 | `/ingest` answered 202 before Kafka had the reading | Broker down → client told "accepted", **reading dropped 5 min later** | Answer 202 only after confirmation, 503 otherwise |
| 6 | Smaller issues (simulator, scripts, config, structure, tests) | See below | See below |

The common thread: **each stage said "done" before the next stage actually
had the data.** Every fix moves that moment to after a confirmation.

---

## 1. Half-processed messages lost their alerts

**What the code assumed.** The consumer is idempotent: if a reading
already exists (unique constraint on `station_id + fuel_type + timestamp`),
a redelivered message is skipped as "already processed — its alerts were
generated the first time."

**What actually happened.** Processing one message ran 4–6 separate
commits: the station, then the reading, then one commit per alert. A crash
after the reading's commit left the reading saved without (all of) its
alerts. The retry then hit the unique constraint and was skipped, so the
missing alerts were never created.

Reproduced with an empty-tank reading (should produce LOW_STOCK +
STATION_CRITICAL) and a crash injected after the first alert:

```
before fix:  after crash → readings=1 alerts=1    after retry → readings=1 alerts=1   (STATION_CRITICAL lost)
after fix:   after crash → readings=0 alerts=0    after retry → readings=1 alerts=2
```

**Why.** The duplicate check treats "the reading exists" as proof that
"the reading *and its alerts* were handled". That's only true if both are
committed atomically — in the same transaction.

**Fix.** The storage helpers (`get_or_create_station`,
`store_fuel_data_idempotent`, `create_alert`) now `flush()` instead of
`commit()` — the SQL is sent and constraints are checked, but nothing is
final. `process_message` commits once at the end, so a failure anywhere
rolls back the reading together with its alerts. The caller owns the
transaction (the *unit of work* pattern), not the helpers.

## 2. Failed messages were skipped, not retried

**What the code assumed.** On a transient failure, `process_message`
returned False and the offset wasn't committed. The log said *"Transient
failure, will retry on restart."*

**What actually happened.** Two Kafka facts made that untrue:

- The consumer's in-memory position had already moved past the failed
  message, so the next `poll()` returned the *next* message, not a retry.
- A committed offset is a single bookmark per partition meaning "everything
  before here is done". Committing the next message on the partition
  therefore marked the failed one as done too — even across restarts.

Reproduced with the real consumer loop, two readings for one tank, and
the database failing on the first write:

```
before fix:  12:00 fails → 12:10 stored and committed → stored readings: 1 of 2 (12:00 lost permanently)
after fix:   12:00 fails → retried after 1s → stored → 12:10 stored → 2 of 2, in order
```

**Fix.** On failure the consumer `seek()`s back to the failed offset and
waits before the next poll (1s, 2s, 4s… capped at 30s, well under Kafka's
5-minute `max.poll.interval.ms`). Later readings for the same tank can't
overtake it, so per-tank ordering is preserved.

Retrying in place raised a new question: what about a message that will
*never* succeed (a bug, a value the database rejects)? Retrying it forever
would block every later reading on its partition — a **poison message**.
Failures are now split in two:

| Error | Handling |
|---|---|
| `OperationalError` (database unreachable) | retried indefinitely — the data is valid and must not be lost |
| anything else | retried `MAX_ATTEMPTS` (5) times, then sent to the DLQ with the error, and the consumer moves on |

## 3. Producer retries could reorder or duplicate readings

**The problem.** For throughput, a producer sends several batches without
waiting for each acknowledgement. With plain retries, if batch 1 fails
transiently and batch 2 succeeds, the retried batch 1 lands *after* batch
2 — exactly the reordering the per-tank message key is there to prevent
(restock detection compares each reading to the previous one). A retry
after a lost acknowledgement also writes the message twice.

**Fix.** `enable.idempotence: True` in the shared producer. The broker
assigns the producer an ID and tracks a sequence number per partition: it
drops duplicates and refuses out-of-order writes. Kafka's Java client has
enabled this by default since 3.0; **librdkafka (used by confluent-kafka)
does not**, which is why it had to be set explicitly. Verified with
librdkafka's `eos` debug log: `Acquired PID{Id:0,Epoch:0}`.

## 4. DLQ writes weren't confirmed before committing

**The problem.** `produce()` only queues a message locally. The consumer
queued the DLQ copy and committed past the original immediately; if the
DLQ write then failed, the message was in neither topic, with no error
anywhere.

**Fix.** `_send_to_dlq` waits for the delivery callback and returns True
only if the broker confirmed the write; otherwise the original isn't
committed and gets retried. The DLQ producer's `message.timeout.ms` is
bounded (10s) so a write that fails is actually dropped, rather than
delivered late *and* again on retry. Verified by pointing the DLQ producer
at a port with no broker: `False` after 10.0s, nothing left queued.

## 5. `/ingest` returned 202 when Kafka was down

**The problem.** `/ingest` queued the reading locally and returned `202
Accepted` immediately. With the broker down, the client was told the
reading was accepted, and the producer dropped it about 5 minutes later
(`message.timeout.ms`), with only a log line. The existing 503 handling
only covered a *full* local queue.

```
before fix:  Kafka down → HTTP 202 after 0.0s, reading still undelivered 8s later
after fix:   Kafka down → HTTP 503 after 5.0s   |   Kafka up → HTTP 202
```

**Fix.** A new `ReadingsProducer.send_and_wait()` gives the message its own
delivery callback and waits for it (up to 5s). It deliberately doesn't use
`flush()`: the producer is shared by every request, and `flush()` would
make each request wait for everyone else's messages (40 concurrent
requests: all 202, about 1s total). The station simulator keeps the
fire-and-forget `send()`.

A request that times out may still be delivered later, and a client retry
then publishes the reading twice. That's harmless by design: the consumer
stores each `(station_id, fuel_type, timestamp)` once.

## 6. Smaller issues found along the way

- **Simulator `--sink http` reported every success as an error.** It
  accepted only 200/201, but `/ingest` has returned 202 since the
  migration, so every reading printed `❌ Error 202`.
- **`start-kafka.sh` waited a fixed 5 seconds** before creating topics. On
  this machine the broker took about 9 seconds to be ready, so topic
  creation could fail. It now polls until the broker answers (up to 60s).
- **The Kafka scripts weren't executable in git** (`100644`), so the
  README's `./scripts/start-kafka.sh` failed with "Permission denied" on a
  fresh clone.
- **The broker address was hard-coded** to `localhost:9092`. It now comes
  from `KAFKA_BOOTSTRAP_SERVERS` (needed for Docker, where it's `kafka:9092`).
- **Alert rules lived in a FastAPI route module**, imported by the consumer
  process. They moved to `backend/services/alerts.py`.
- **No automated tests.** There is now a pytest suite (below).

---

## How it's tested now

`pytest` runs 30 tests in a few seconds against an in-memory SQLite
database, with small fakes for Kafka that mimic real offset semantics
(`poll` moves the position, `seek` moves it back, `commit` records
"next to read"). One integration test uses a real broker (skipped when
none is running): two readings, the database failing on the first writes,
both stored in order.

A test suite that has only ever passed proves little, so each fix was
checked by **putting the bug back and confirming a test fails**:

| Bug reintroduced | Tests that fail |
|---|---|
| Commit inside the storage helper (no single transaction) | 3 |
| No seek-back after a failure | 3 |
| Database errors treated like bugs (sent to the DLQ) | 1 |
| Producer not idempotent | 1 |
| Commit without waiting for the DLQ confirmation | 2 |
| `/ingest` fire-and-forget | 4 |

## Pitfalls hit while testing

These weren't bugs in the project, but each one briefly made a correct
result look wrong (or the reverse):

- **A first reproduction that couldn't tell the difference.** The first
  crash test used a reading that produces only one alert, so "before" and
  "after" printed the same totals. An empty tank (two alerts) made the
  lost alert visible. A reproduction is only useful if the buggy and fixed
  behaviors produce *different* output.
- **"The script hangs with no output."** It didn't: `timeout` signals the
  whole pipeline, so the `sed`/`grep` reading the script's output was
  killed too, taking its buffered output with it. Writing to a file
  instead of piping showed the script had worked all along.
- **`Coordinator load in progress`** on the first idempotent producer:
  the broker was still initializing; librdkafka retried and got a producer
  ID moments later. Harmless, but alarming in a debug log.
- **A leftover test topic.** Kafka admin operations are asynchronous;
  `delete_topics()` returned before the deletion happened, and the test
  process exited first. The fixture now waits on the result.

## The resulting guarantees

| Stage | Says "done" only after… |
|---|---|
| `/ingest` → client | Kafka confirms the write (202), otherwise 503 |
| Producer retries | the broker deduplicates and enforces order (idempotence) |
| Consumer → database | the reading **and** its alerts commit together |
| Consumer → Kafka offset | the database commit succeeded, or the message is confirmed in the DLQ |

Together this is **at-least-once delivery** — nothing is lost, but a
message can be seen more than once — made safe by an **idempotent
consumer**. That combination is the standard way to get correct results
from Kafka without the cost of exactly-once transactions.

## Known limits (accepted)

- **Single broker, replication factor 1** — fine for local development,
  not durable against a broker's disk failing (see the ADR).
- **The retry wait pauses all partitions**, not just the failing one. The
  main transient failure (database down) affects every partition anyway;
  per-partition `pause()`/`resume()` would be more complex for no gain here.
- **The DLQ can contain a duplicate** if the consumer crashes after the DLQ
  write is confirmed but before its offset is committed. Acceptable for a
  topic meant for human inspection.
- **The tests use SQLite**, so Postgres-specific behavior (like the
  [timezone bug](timestamp-timezone-bug.md)) isn't covered by them.

## Takeaway

The code already *described* a reliable pipeline: comments promised retries
on restart, a DLQ for failures, a 503 when Kafka couldn't keep up. What it
didn't have was anyone watching those failures happen. Each problem here
was invisible on the happy path and obvious within seconds of injecting the
failure it was supposed to handle. A reliability property is only real once
you've seen the system survive the failure it's meant to survive, and then
seen a test fail when the fix is removed.
