# Demo script: Kafka ingestion pipeline

A ~5-minute walkthrough for showing this project live — e.g. in an
internship interview or portfolio review. Assumes everything is already
set up locally (see README "Running"). Total setup time before an
interview: ~2 minutes to start all services.

## Before the call

Start these in order, each in its own terminal, and leave them all running
and visible (tile the terminals if possible — watching things happen live
across multiple windows is the actual demo):

```bash
./scripts/start-kafka.sh
uvicorn backend.main:app --reload --port 8000
python -m ingestion_consumer.main
cd stations && python agilAgentStation.py --sink kafka
```

Also open a live Kafka consumer, purely for visual effect — this is the
single most convincing thing you can show, because it makes an invisible
system visible:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic fuel.readings.raw
```

## The walkthrough

**1. Start with the "why" (30 seconds), not the "what."**
"The original version of this had `/ingest` write straight to Postgres in
the same HTTP request. That coupled ingestion throughput to database
throughput, and there was no way to replay history if I found a bug in the
processing logic later. I moved it to Kafka so ingestion and storage are
decoupled, and the event log is replayable."

**2. Show data actually flowing (this is the payoff).**
Point at the three simulator stations printing `Queued: ...` lines, then
point at the live Kafka consumer terminal showing the same readings
arriving as raw JSON, then point at the ingestion consumer's terminal
showing `Stored reading: ...` lines a moment later. Narrate the hop:
"simulator → Kafka → consumer → Postgres, three independent processes,
none of them aware of each other beyond the topic."

**3. Prove it's not just the simulator — do a live `curl`.**
```bash
curl -X POST http://localhost:8000/ingest/ \
  -H "Content-Type: application/json" \
  -d '{
    "timestamp": "2026-01-01T12:00:00Z",
    "station_id": "BI00001",
    "fuel_type": "Gasoil50",
    "price_tnd": 2.55,
    "official_price_tnd": 2.55,
    "stock_liters": 8500,
    "capacity_liters": 10000,
    "sales_last_5min_liters": 12
  }'
```
Point out the response: `202 Accepted`, no `id` field. "This isn't stored
yet — it's queued. The response is honest about that." Then show it land
in the live consumer terminal, then show it appear via `/history` a moment
later. "Two independent producers — the simulator and the API — publish
into the exact same topic, keyed the same way, so they can't ever get out
of order relative to each other for the same tank."

**4. Show idempotency (the detail most people never demo).**
Run the exact same `curl` command again, unchanged. "Same request, sent
twice — this is what 'at-least-once delivery' means in practice, a
message can be redelivered. It doesn't create a duplicate row." Then show
the row count in `/history` didn't change, or point at the unique
constraint via `psql`.

**5. Show a failure mode on purpose.**
Publish something deliberately broken directly to the topic:
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --topic fuel.readings.raw \
  --bootstrap-server localhost:9092
```
Type `not valid json` and hit enter. Point at the consumer's terminal
logging it as routed to the DLQ, not crashing. Then show it sitting in the
DLQ topic:
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic fuel.readings.dlq --from-beginning
```
"One bad message doesn't take down the pipeline, and it isn't silently
dropped either — it's sitting here, inspectable."

**6. If asked "why Kafka and not just a task queue" — have this ready.**
"I actually wrote this up as an ADR" — open `docs/adr/0001-kafka-ingestion.md`
— "the short version: a queue like Celery/RabbitMQ removes a message once
it's acknowledged. I specifically wanted a retained, replayable log, which
is Kafka's core model, not a queue's."

**7. If there's time, tell the debugging story.**
This is genuinely strong material — open `docs/debugging/timestamp-timezone-bug.md`.
"I actually hit a real bug during this migration — timestamps were coming
back an hour off. I want to walk through how I actually found it, because
the interesting part wasn't the fix, it was that my first several
reproduction attempts were testing the wrong database engine entirely."
Walk through the elimination process briefly: traced every hop of the
pipeline (all correct), suspected SQLAlchemy/SQLite (read the actual
source, proved it does no timezone math), only then realized the running
system was Postgres, not SQLite — and psycopg2 silently converts
timezone-aware datetimes to the session's local timezone before storing
them in a `TIMESTAMP WITHOUT TIME ZONE` column. Fixed by using
`TIMESTAMPTZ` everywhere. "The lesson for me was that I'd verified a lot
of code correctly without questioning an environment assumption underneath
all of it."

**8. If asked "what happens when something fails?" — show it.**
Stop Kafka (`docker stop kafka`) and send the same `curl` as in step 3:
it now answers `503` after about 5 seconds instead of `202`. "The API
only says 'accepted' once Kafka has confirmed it has the reading;
otherwise the client is told to retry." Restart Kafka (`docker start
kafka`) and the same request returns `202` again.

Then the consumer side, without live surgery: run `pytest
tests/test_consumer.py -v` and point at the test names
(`test_failed_message_is_retried_not_skipped`,
`test_crash_between_alerts_rolls_back_the_reading`,
`test_poison_message_is_parked_and_the_rest_flows`). "Each of these was a
real bug — the code's comments promised retries, but a failed message was
actually skipped for good. I reproduced each one before fixing it, and
checked that every test fails if the fix is removed." The full story is in
`docs/debugging/kafka-reliability-review.md`.

## If something doesn't come up cleanly

- **Kafka container not running** (`docker ps` shows nothing): `./scripts/start-kafka.sh`.
- **Consumer shows `Connection refused` repeatedly**: same cause, start Kafka first, then restart the consumer.
- **`/history` shows nothing after a `curl`**: check the consumer's terminal for either a DLQ routing line or a transient-failure line before assuming Kafka itself is broken.

## What NOT to over-explain

Don't lead with Kafka internals (partitions, consumer groups, KRaft) unless
asked — lead with the *behavior* (replay, decoupling, idempotency,
graceful failure) and let questions pull you into the internals. The
behavior is what's actually impressive; the internals are what prove you
understand *why* it works, which is the natural follow-up question, not
the opening pitch.