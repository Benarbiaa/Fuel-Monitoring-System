"""
Decides and executes the automated response to one critical alert.

The watcher (watcher.py) claims an alert and calls process_alert(). Every
outcome ends in a defined state, never silently:

- success                     → "acknowledged", handled_by="agent"
- LLM or action failed        → "retrying" with a backoff delay, error kept
                                in last_error, until MAX_ATTEMPTS
- attempts exhausted, or the  → escalated to a human: "acknowledged",
  LLM can't work at all         handled_by="fallback", reason in the log
  (no key, rejected key)

A critical alert is never closed without an action being recorded in
incident_logs, and the incident log and the status change are committed
together, so the audit trail can't disagree with the alert's status.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy.orm import Session

from backend.agent.actions import ALLOWED_ACTIONS, execute_action
from backend.database import models
from backend.database.database import SessionLocal

logger = logging.getLogger(__name__)

# Total tries (LLM call + action) before falling back to escalation. With
# the backoff below, that's about 7.5 minutes of retrying.
MAX_ATTEMPTS = 5
# Wait before the next try: 30s, 1m, 2m, 4m... capped at 10 minutes.
RETRY_BASE_SECONDS = 30
RETRY_MAX_SECONDS = 600


class PermanentLLMError(Exception):
    """The LLM can't be used at all right now (missing or rejected API key).
    Retrying won't help until someone fixes the configuration."""


async def decide_action(alert: models.Alert) -> tuple[str, str]:
    """
    Asks the LLM which action to take. Returns (action, reason), where
    action is always one of ALLOWED_ACTIONS. Raises PermanentLLMError for
    configuration problems, and any other exception (timeout, rate limit,
    server error) for failures worth retrying.
    """
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise PermanentLLMError("GROQ_API_KEY not configured")

    context = {
        "alert_id": alert.id,
        "station_id": alert.station_id,
        "fuel_type": alert.fuel_type,
        "alert_type": alert.alert_type,
        "severity": alert.severity,
        "message": alert.message,
        "timestamp": alert.timestamp.isoformat() if alert.timestamp else None,
    }

    prompt = (
        "Alert Context:\n"
        + json.dumps(context, indent=2)
        + "\n\nChoose ONE action:\n"
        '1. "reorder" - stock is critically low\n'
        '2. "notify_manager" - price anomaly or high consumption\n'
        '3. "escalate" - critical situation needing human intervention\n\n'
        'Respond with ONLY JSON: {"action": "...", "reason": "..."}'
    )

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "openai/gpt-oss-20b",  # llama-3.1-8b-instant was deprecated by Groq on 2026-08-16
                "messages": [
                    {
                        "role": "system",
                        "content": 'You are an autonomous fuel station monitoring agent. Choose exactly one action: reorder, notify_manager, or escalate. Respond ONLY with valid JSON: {"action": "...", "reason": "..."} No extra text outside the JSON.'
                    },
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": 256,
                "temperature": 0.2,
            },
        )
    if response.status_code in (401, 403):
        raise PermanentLLMError(f"Groq rejected the API key (HTTP {response.status_code})")
    if response.status_code != 200:
        logger.error(f"Groq error body: {response.text}")
    response.raise_for_status()

    response_text = response.json()["choices"][0]["message"]["content"].strip()
    logger.info(f"LLM response: {response_text}")

    # The LLM answered but not usefully: don't retry (it would likely do the
    # same again), escalate so a human decides instead.
    try:
        parsed = json.loads(response_text)
    except json.JSONDecodeError:
        logger.error(f"Failed to parse response: {response_text}")
        return "escalate", "Failed to parse LLM response; escalating to a human."

    action = parsed.get("action", "escalate")
    reason = parsed.get("reason", "No reason provided")
    if action not in ALLOWED_ACTIONS:
        # LLM output is untrusted input: never log an action that wasn't taken.
        logger.warning(f"LLM chose unknown action {action!r}; escalating instead")
        return "escalate", f"LLM chose unknown action {action!r} ({reason}); escalating to a human."
    return action, reason


def _retry_delay_seconds(attempts: int) -> int:
    return min(RETRY_BASE_SECONDS * 2 ** (attempts - 1), RETRY_MAX_SECONDS)


def _schedule_retry(db: Session, alert: models.Alert, error: Exception) -> None:
    delay = _retry_delay_seconds(alert.attempts)
    alert.status = "retrying"
    alert.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
    alert.last_error = f"{type(error).__name__}: {error}"[:500]
    db.commit()
    logger.warning(
        f"Alert {alert.id}: attempt {alert.attempts}/{MAX_ATTEMPTS} failed "
        f"({alert.last_error}); retrying in {delay}s"
    )


def _mark_handled(alert: models.Alert, handled_by: str) -> None:
    alert.status = "acknowledged"
    alert.handled_by = handled_by
    alert.handled_at = datetime.now(timezone.utc)
    alert.next_attempt_at = None


async def process_alert(alert_id: int):
    """
    Decides and executes the response to one alert the watcher has claimed
    (status "processing", attempts already incremented). Opens its own DB
    session to avoid issues with async/await boundaries.
    """
    db: Session = SessionLocal()
    try:
        alert = db.query(models.Alert).filter(models.Alert.id == alert_id).first()
        if not alert:
            logger.error(f"Alert {alert_id} not found")
            return

        logger.info(f"Processing alert {alert.id} (attempt {alert.attempts}): {alert.alert_type} at {alert.station_id}")

        try:
            try:
                action, reason = await decide_action(alert)
                handled_by = "agent"
            except PermanentLLMError as e:
                logger.error(f"Alert {alert.id}: {e}; escalating by fallback")
                action, reason = "escalate", f"{e}; escalated by fallback without an LLM decision."
                handled_by = "fallback"
            await execute_action(action, alert, db, reason, actor=handled_by)

        except Exception as e:
            # The LLM call or the action itself failed (timeout, rate limit,
            # mail server down...). Undo anything the action added, then
            # retry later, or give up and escalate once attempts run out.
            db.rollback()
            if alert.attempts < MAX_ATTEMPTS:
                _schedule_retry(db, alert, e)
                return
            logger.error(f"Alert {alert.id}: giving up after {alert.attempts} attempts ({e}); escalating")
            action, handled_by = "escalate", "fallback"
            reason = (f"Automated handling failed {alert.attempts} times "
                      f"(last error: {type(e).__name__}: {e}); escalating to a human.")
            alert.last_error = f"{type(e).__name__}: {e}"[:500]
            await execute_action(action, alert, db, reason, actor=handled_by)

        # The incident log (added by execute_action) and the new status are
        # committed together: the audit trail never disagrees with the alert.
        _mark_handled(alert, handled_by)
        db.commit()
        logger.info(f"Alert {alert.id} handled by {handled_by} with action: {action}")

    except Exception:
        # Even the fallback failed (most likely the database itself). The
        # alert stays "processing" and is picked up again once its lease
        # expires (see watcher.py), so it isn't lost.
        logger.exception(f"Alert {alert_id}: could not record the outcome; will be retried")
        db.rollback()
    finally:
        db.close()
