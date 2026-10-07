"""
The automation agent (backend/agent/): how the watcher claims critical
alerts and how every outcome of handling one is recorded. The LLM is
faked — these tests never call the Groq API. See
docs/debugging/alert-agent-review.md for the bugs they guard against.
"""

import asyncio
import smtplib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest

import backend.agent.actions as actions
import backend.agent.responder as responder
import backend.agent.watcher as watcher
from backend.database import models
from tests.conftest import STATION

# Real time, not a fixed date: the responder schedules retries from the real
# clock, so the "now" passed to the watcher has to be on the same clock.
NOW = datetime.now(timezone.utc)


@pytest.fixture
def agent_db(session_factory, monkeypatch):
    monkeypatch.setattr(responder, "SessionLocal", session_factory)
    monkeypatch.setattr(watcher, "SessionLocal", session_factory)
    monkeypatch.setenv("GROQ_API_KEY", "fake-key")
    for var in ("SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS", "MANAGER_EMAIL"):
        monkeypatch.delenv(var, raising=False)
    db = session_factory()
    db.add(models.Station(station_id=STATION, company="AGIL", location="Test"))
    db.commit()
    yield db
    db.close()


def add_alert(db, severity="critical", status="new", **fields):
    alert = models.Alert(station_id=STATION, fuel_type="Gasoil50", alert_type="LOW_STOCK",
                         severity=severity, message="stock at 2%", status=status, **fields)
    db.add(alert)
    db.commit()
    return alert.id


def llm(monkeypatch, status=200, content=None, exc=None):
    """Fakes the Groq HTTP call."""
    async def fake_post(self, url, **kwargs):
        if exc:
            raise exc
        body = {"choices": [{"message": {"content": content}}]} if content else {"error": "x"}
        return httpx.Response(status, json=body, request=httpx.Request("POST", url))
    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)


def poll(now=NOW):
    return asyncio.run(watcher.run_once(now))


def alert_state(db, alert_id):
    db.expire_all()
    alert = db.get(models.Alert, alert_id)
    incidents = [(i.action, i.actor) for i in
                 db.query(models.IncidentLog).filter_by(alert_id=alert_id).order_by(models.IncidentLog.id)]
    return alert, incidents


# --- outcomes of one attempt -------------------------------------------------

def test_llm_decision_is_executed_and_logged(agent_db, monkeypatch):
    llm(monkeypatch, content='{"action": "reorder", "reason": "stock low"}')
    alert_id = add_alert(agent_db)
    assert poll() == 1
    alert, incidents = alert_state(agent_db, alert_id)
    assert (alert.status, alert.handled_by) == ("acknowledged", "agent")
    assert incidents == [("reorder", "agent")]


@pytest.mark.parametrize("failure", [
    dict(status=429), dict(status=500), dict(exc=httpx.ReadTimeout("timed out")),
], ids=["rate-limited", "server-error", "timeout"])
def test_llm_failure_is_retried_later_not_closed(agent_db, monkeypatch, failure):
    # Before the fix: "acknowledged" by "system", with no action taken.
    llm(monkeypatch, **failure)
    alert_id = add_alert(agent_db)
    poll()
    alert, incidents = alert_state(agent_db, alert_id)
    assert alert.status == "retrying"
    assert alert.attempts == 1
    assert alert.last_error
    assert incidents == []


def test_retry_waits_for_its_backoff(agent_db, monkeypatch):
    llm(monkeypatch, status=429)
    add_alert(agent_db)
    poll(NOW)
    assert poll(NOW + timedelta(seconds=10)) == 0    # too early
    assert poll(NOW + timedelta(seconds=31)) == 1    # first retry after 30s


def test_backoff_schedule():
    delays = [responder._retry_delay_seconds(n) for n in range(1, 8)]
    assert delays == [30, 60, 120, 240, 480, 600, 600]


def test_gives_up_and_escalates_after_max_attempts(agent_db, monkeypatch):
    llm(monkeypatch, status=429)
    alert_id = add_alert(agent_db)
    now = NOW
    for _ in range(responder.MAX_ATTEMPTS):
        assert poll(now) == 1
        now += timedelta(hours=1)
    alert, incidents = alert_state(agent_db, alert_id)
    assert (alert.status, alert.handled_by) == ("acknowledged", "fallback")
    assert alert.attempts == responder.MAX_ATTEMPTS
    assert incidents == [("escalate", "fallback")]
    assert poll(now) == 0   # done: never picked up again


@pytest.mark.parametrize("setup", ["rejected-key", "missing-key"])
def test_unusable_llm_escalates_immediately(agent_db, monkeypatch, setup):
    # Before the fix, a rejected key left the alert in "processing" forever.
    if setup == "rejected-key":
        llm(monkeypatch, status=401)
    else:
        monkeypatch.delenv("GROQ_API_KEY")
    alert_id = add_alert(agent_db)
    poll()
    alert, incidents = alert_state(agent_db, alert_id)
    assert (alert.status, alert.handled_by, alert.attempts) == ("acknowledged", "fallback", 1)
    assert incidents == [("escalate", "fallback")]


def test_unknown_llm_action_is_escalated_not_logged_as_done(agent_db, monkeypatch):
    llm(monkeypatch, content='{"action": "shutdown_station", "reason": "x"}')
    alert_id = add_alert(agent_db)
    poll()
    _, incidents = alert_state(agent_db, alert_id)
    assert incidents == [("escalate", "agent")]


def test_failed_manager_email_is_retried_not_logged_as_sent(agent_db, monkeypatch):
    llm(monkeypatch, content='{"action": "notify_manager", "reason": "price anomaly"}')
    monkeypatch.setenv("SMTP_HOST", "smtp.invalid")
    monkeypatch.setenv("SMTP_PORT", "587")
    monkeypatch.setenv("SMTP_USER", "user")
    monkeypatch.setenv("SMTP_PASS", "pass")
    monkeypatch.setenv("MANAGER_EMAIL", "manager@example.com")

    def mail_server_down(*args, **kwargs):
        raise smtplib.SMTPConnectError(421, "mail server down")

    monkeypatch.setattr(smtplib, "SMTP", mail_server_down)
    alert_id = add_alert(agent_db)
    poll()
    alert, incidents = alert_state(agent_db, alert_id)
    assert alert.status == "retrying"
    assert "SMTPConnectError" in alert.last_error
    assert incidents == []


def test_incident_log_and_status_are_saved_together(agent_db, monkeypatch):
    # If the incident log can't be written, the alert must not end up
    # "acknowledged" with no record of what was done.
    llm(monkeypatch, content='{"action": "reorder", "reason": "stock low"}')

    def broken_incident_log(**kwargs):
        raise RuntimeError("incident_logs insert failed")

    # Replace only actions.py's reference to the models module; patching
    # models.IncidentLog itself would also break this test's own queries.
    monkeypatch.setattr(actions, "models", SimpleNamespace(IncidentLog=broken_incident_log))
    alert_id = add_alert(agent_db)
    poll()
    alert, _ = alert_state(agent_db, alert_id)
    assert alert.status == "retrying"
    assert alert.handled_at is None


def test_no_incident_is_recorded_if_the_status_update_fails(agent_db, monkeypatch):
    # The other direction: the action and its incident log succeed, but
    # saving the alert's new status fails. Committed separately, the log
    # would say "done" for an alert that's still open (and the retry would
    # log it a second time). Committed together, both are rolled back and
    # the alert is picked up again when its lease expires.
    llm(monkeypatch, content='{"action": "reorder", "reason": "stock low"}')

    def status_update_fails(alert, handled_by):
        raise RuntimeError("database went away")

    monkeypatch.setattr(responder, "_mark_handled", status_update_fails)
    alert_id = add_alert(agent_db)
    poll()
    alert, incidents = alert_state(agent_db, alert_id)
    assert alert.status == "processing"   # lease expiry will retry it
    assert incidents == []


# --- claiming ----------------------------------------------------------------

def test_warnings_are_not_handled_by_the_agent(agent_db, monkeypatch):
    llm(monkeypatch, content='{"action": "reorder", "reason": "x"}')
    add_alert(agent_db, severity="warning")
    assert poll() == 0


@pytest.mark.parametrize("lease_expires, picked_up", [
    (NOW + timedelta(minutes=2), False),   # still being worked on
    (NOW - timedelta(seconds=1), True),    # the worker died: lease expired
    (None, True),                          # stuck before leases existed
], ids=["active-lease", "expired-lease", "legacy-stuck"])
def test_processing_alert_is_picked_up_again_once_its_lease_expires(
        agent_db, monkeypatch, lease_expires, picked_up):
    # Before the fix, a restart mid-processing left the alert stuck forever.
    llm(monkeypatch, content='{"action": "reorder", "reason": "x"}')
    alert_id = add_alert(agent_db, status="processing", attempts=1, next_attempt_at=lease_expires)
    assert poll() == (1 if picked_up else 0)
    alert, _ = alert_state(agent_db, alert_id)
    assert alert.status == ("acknowledged" if picked_up else "processing")
