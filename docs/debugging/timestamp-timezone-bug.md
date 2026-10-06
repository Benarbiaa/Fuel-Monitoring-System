# Debugging story: the case of the vanishing hour

**Symptom:** a fuel reading submitted with `timestamp: "2026-09-18T12:00:00Z"`
came back from `GET /history` as `"2026-09-18T13:00:00"` — exactly one hour
later, and with no timezone offset shown at all.

This is a writeup of how that bug was actually found, because the process
matters more than the one-line fix: the obvious suspects were all innocent,
and the real cause was two layers removed from where it first looked like
it lived.

## First hypothesis: something in our own ingestion code

The system had just been migrated to a Kafka-based pipeline (station
simulator / `POST /ingest` → Kafka → a consumer service → Postgres), so the
first suspicion was a mistake introduced during that migration — maybe a
serialization step silently converting the timestamp somewhere along the
new path.

To find out, every hop was traced independently, in isolation, using the
actual project code:

1. Pydantic parsing the incoming string → `12:00:00, tzinfo=UTC`
2. `.model_dump(mode="json")` (what gets published to Kafka) → `"2026-09-18T12:00:00Z"`
3. `json.dumps()` onto the wire → unchanged
4. The consumer's `json.loads()` + re-validation → `12:00:00, tzinfo=UTC`
5. `.dict()`, building the SQLAlchemy model kwargs → still `12:00:00, tzinfo=UTC`

Every step was correct. The value only went wrong *after* it was handed to
SQLAlchemy — which meant the Kafka migration itself was innocent. This
was worth confirming explicitly rather than assuming, since it would have
been easy to blame the newest, most complex-looking part of the system
when it turned out to be blameless.

## Second hypothesis: SQLAlchemy / SQLite

The next suspect was the ORM layer itself — maybe something in how
SQLAlchemy binds a timezone-aware `datetime` was silently converting it.

This was tested directly: SQLAlchemy's actual SQLite `DATETIME` bind
processor source was read (not guessed at), which confirmed it reads
`value.hour`, `value.minute`, etc. directly off the Python object and
performs **no timezone math at all** — it's timezone-*blind*, not
timezone-*converting*. A local reproduction (writing the exact same value
through the exact same model, against a real SQLite database) confirmed
this: no shift occurred.

Several more targeted eliminations followed the same pattern — install the
project's exact pinned `pydantic` and `sqlalchemy` versions, simulate the
system's local timezone (`TZ=Africa/Tunis`) via environment variable, rerun
the reproduction. None of it reproduced the bug. At this point, every piece
of *our own code*, run under conditions matching the report as closely as
this environment allowed, was behaving correctly.

## The detail that had been assumed, not verified

The investigation had quietly been assuming the project was running on
SQLite (its documented local-dev fallback when no `DATABASE_URL` is set).
It wasn't — the project was actually running against **Postgres**. That
single wrong assumption is what made every SQLite-based reproduction
attempt look clean: the bug was never in SQLite's code path at all.

This is the actual lesson of this story: several hours of careful,
methodical elimination were spent verifying code that was never going to
reproduce the bug, because the environment assumption underneath all of it
was wrong. The fix for *that* wasn't a better reproduction technique — it
was going back and asking a more basic question about the environment
before trusting the results of testing it.

## The real cause

`FuelData.timestamp` was declared as `Column(DateTime, ...)` — with no
`timezone=True`. In Postgres, that maps to `TIMESTAMP WITHOUT TIME ZONE`.

`psycopg2`'s handling of this case is well-documented but easy to never
encounter until it bites you: when a timezone-*aware* Python `datetime` is
bound to a `TIMESTAMP WITHOUT TIME ZONE` column, psycopg2 doesn't simply
strip the offset and keep the wall-clock numbers (which is what SQLite
does, and what was implicitly expected from the earlier testing). Instead,
it first **converts the value to the Postgres session's configured local
timezone**, and only then strips the (now-irrelevant) offset. If the
server's session timezone defaults to the system locale — `Africa/Tunis`,
UTC+1, exactly matching the observed shift — a `12:00 UTC` input silently
becomes a stored `13:00`, with no record that any conversion happened at
all.

This explained every observation:
- Why it was invisible in every SQLite-based test (SQLite has no
  session-timezone concept to convert through).
- Why the shift was exactly +1 hour (the server's local UTC offset).
- Why the stored value had no offset marker (the column type has none to
  store).

## The fix

Every timestamp column in `backend/database/models.py` was changed from
`DateTime` to `DateTime(timezone=True)`, mapping to Postgres `TIMESTAMPTZ`.
A `TIMESTAMPTZ` column always stores the instant internally as true UTC,
regardless of session timezone, and correctly round-trips a timezone-aware
Python `datetime` without any silent conversion.

Because the project creates tables via `Base.metadata.create_all()` rather
than migrations, this change didn't take effect on the existing database —
`create_all()` only creates tables that don't already exist; it never
alters an existing column's type. The fix required dropping and recreating
the affected tables. (This is the second time a schema change in this
project has hit that exact limitation — see "Known Limitations" in the
README for why Alembic migrations are the real long-term fix.)

## Aftermath: is `+01:00` in API responses correct?

After the fix, a reading submitted as `15:00:00Z` was returned by the API
as `16:00:00+01:00`. This looks alarming at first glance but is actually
correct: `16:00+01:00` and `15:00Z` denote the *exact same instant* — the
value is now round-tripping through a session-timezone-aware display
format rather than a silently-stripped one. The bug was never about the
displayed hour; it was about that offset being **dropped** rather than
**shown**.

It was still worth normalizing, since the offset in API responses depended
on whichever timezone the database server happened to run in. That
follow-up is done: the engine now pins every Postgres session to UTC
(`options=-c timezone=UTC` in `backend/database/database.py`), so the same
reading now comes back as `15:00:00Z`, regardless of server locale.

## Takeaway

The most expensive mistake in this investigation wasn't a wrong guess
about a library's behavior — every technical hypothesis tested was tested
correctly and ruled out correctly. The expensive mistake was an
unquestioned assumption about *which database was even in use*, made
before any testing started, that silently invalidated every subsequent
(individually correct) reproduction attempt. When a careful investigation
keeps returning "everything looks correct" against a real, live bug, the
environment assumptions underneath the investigation deserve as much
scrutiny as the code itself.