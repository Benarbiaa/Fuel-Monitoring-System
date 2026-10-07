# Alert agent review: what happens when the LLM (or anything else) fails?

The automation agent watches for critical alerts, asks an LLM which action
to take (`reorder`, `notify_manager` or `escalate`), executes it and logs
it to `incident_logs`. On the happy path this worked. This review asked
the same question as the [Kafka review](kafka-reliability-review.md): what
happens when a step fails? For an agent built on a rate-limited external
LLM, failure isn't an edge case — it's a normal Tuesday.

## How it was reproduced

A script ran the real watcher, responder and actions code against an
in-memory database, with the LLM's HTTP call faked (no Groq requests), and
put one critical alert ("stock at 2%") through each scenario:

| Scenario | Before | After |
|---|---|---|
| LLM decides `reorder` | `acknowledged` by agent, incident `reorder` | same |
| LLM rate-limited (429) | **`acknowledged` by "system", no action, no incident** | `retrying`, error kept |
| LLM timeout | **same as above** | `retrying`, error kept |
| API key rejected (401) | **`processing` forever** | escalated by fallback |
| LLM invents `shutdown_station` | **`acknowledged`, incident says `shutdown_station`** | escalated, incident says `escalate` |
| Email to the manager fails | **`acknowledged`, incident says `notify_manager`** | `retrying`, error kept |
| Backend restarts mid-processing | **`processing` forever** | picked up again, handled |

Six of seven failure scenarios ended wrong, in three different ways.

## Problem 1: failures were recorded as successes

On any error except 401/403, the responder's `except` block set the alert
to `acknowledged` (`handled_by="system"`). Nothing was done and nothing
was logged, but the alert looked handled. With Groq's free tier, a 429 is
the most likely failure of all, so the most likely failure silently
closed critical alerts.

The same pattern appeared twice more:
- **Email failures were swallowed** (`notify_manager` caught and logged
  them), so `execute_action` returned normally and the incident log
  recorded `notify_manager` for a manager who was never notified.
- **The LLM's chosen action was trusted as-is.** An unknown action only
  triggered a warning, but was still written to the incident log as if it
  had been taken.

**Fix.** An action that fails now raises; the responder catches it and
schedules a retry instead of recording success. LLM output is validated
against the allowed actions, and anything else becomes an escalation.

## Problem 2: alerts could get stuck forever

The watcher only picked up alerts with `status="new"`, and set them to
`processing` before calling the responder. Anything that left an alert in
`processing` made it invisible from then on:
- a 401/403 (the code's comment said *"will be retried next poll"* — but
  the next poll only looks at `new`),
- the backend restarting during the LLM call.

**Fix: a lease.** Claiming an alert now sets `next_attempt_at = now + 5
minutes` along with `status="processing"`. The watcher considers an alert
due when it's `new`, `retrying` with its retry time reached, **or
`processing` with its lease expired**. Nobody has to notice a crash: if
the worker dies, the lease simply runs out and the next poll takes the
alert again. The lease is longer than one attempt can take (30s LLM
timeout + 10s email timeout), so a live attempt is never mistaken for a
dead one. Alerts stuck by the old code (in `processing`, no lease) are
picked up on the first poll.

## Problem 3: retry forever, or give up?

Retrying needs a limit, and the question is what happens at the limit.
For a critical alert, "give up" can't mean "close it". The design:

| Failure | Handling |
|---|---|
| Transient (timeout, 429, 5xx, mail server down) | `retrying` with backoff: 30s, 1m, 2m, 4m (about 7.5 minutes in total) |
| Still failing after 5 attempts | **escalate** — a human takes over — recorded with `handled_by="fallback"` and the last error |
| Permanent (no API key, key rejected) | escalate immediately: retrying won't fix a configuration problem |

Escalation is the safe default because it needs nothing that just failed:
no LLM, no mail server. Attempts are counted when the alert is claimed,
not when it fails, so an alert that crashes the worker every time is also
capped.

## Problem 4: the audit trail could disagree with the alert

`execute_action` committed the incident log on its own; the responder
committed the alert's new status afterwards. A failure between the two
left an incident saying "done" on an alert still open — and when it was
retried, a second incident for the same alert.

**Fix.** The same one as the [consumer's step A](kafka-reliability-review.md#1-half-processed-messages-lost-their-alerts):
`execute_action` only flushes the incident; the responder commits it
together with the status change. Either both are saved or neither is.

## Schema change without migrations

Retries need state: `attempts`, `next_attempt_at`, `last_error`. The
project creates tables with `create_all()`, which never adds columns to an
existing table, so existing databases need
`scripts/migrations/001_alert_retry_columns.sql` (idempotent,
`ADD COLUMN IF NOT EXISTS`). This is the third schema change that
`create_all()` couldn't handle on its own, which makes Alembic the obvious
next step.

## How it's tested

`tests/test_alert_agent.py` (17 tests, LLM faked, in-memory database)
covers every row of the table above, the backoff schedule, the 5-attempt
limit, leases (active, expired, legacy-stuck) and the audit trail staying
consistent in both directions. Each fix was checked by putting the bug
back and confirming a test fails:

| Bug reintroduced | Tests that fail |
|---|---|
| LLM failure closes the alert | 7 |
| Expired leases not picked up | 2 |
| Retry time ignored | 2 |
| Rejected key retried like a transient error | 1 |
| Unknown LLM action accepted | 1 |
| Email failure swallowed | 1 |
| Incident committed separately from the status | 1 |

That last row first came back **0 failing**: the existing test only
covered the incident write failing, not the status update failing after
the incident was saved. The missing test was added. Removing a fix and
watching the suite is what found the gap.

## Pitfalls hit while testing

- **Two clocks.** The first version of the tests passed a fixed date
  (`2026-01-01`) to the watcher, while the responder scheduled retries
  from the real clock (October). Retries looked "never due". The tests
  now use the real time, matching production.
- **A monkeypatch that patched too much.** `actions.models` *is* the
  `models` module, so patching `actions.models.IncidentLog` also broke the
  test's own queries. Patching only `actions`' reference to the module
  fixed it.

## Known limits (accepted)

- **An email can be sent twice.** If the action succeeds but saving the
  outcome fails, the retry runs the action again. Same trade-off as
  Kafka's at-least-once delivery: a duplicate notification is better than
  a missing one for a critical alert.
- **A retry asks the LLM again**, which may choose a different action
  than the attempt that failed. Storing the first decision would avoid
  that; not worth the extra state yet.
- **Alerts are processed one at a time** per poll. Fine at this volume.
- **The status isn't shown in the dashboard yet** (`AlertResponse` doesn't
  include it), so `retrying` and `fallback` are only visible in the
  database and logs.

## Takeaway

The original agent had one `except` block deciding the fate of every
failure, and it chose "acknowledged" — the outcome that looks best and
means least. For anything that acts on someone's behalf, the important
design question isn't the happy path; it's **what state every failure
leaves behind**, and whether a human would find out.
