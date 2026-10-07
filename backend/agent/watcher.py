"""
Background loop that hands critical alerts to the responder.

An alert is "due" when it is critical and either:
- new,
- retrying and its retry time has come, or
- processing but its lease has expired.

Claiming an alert sets it to "processing" with a lease (next_attempt_at =
now + LEASE_SECONDS) and counts an attempt. If the backend dies mid-way,
nothing resets the status, but the lease runs out and the next poll picks
the alert up again, so no alert can stay stuck in "processing" forever.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import or_
from sqlalchemy.orm import Session

from backend.database.database import SessionLocal
from backend.database import models
from backend.agent.responder import process_alert

# Configure logging so messages actually show in terminal
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 30
# Longer than one attempt can take (LLM timeout 30s + email timeout 10s),
# so a live attempt is never mistaken for a dead one.
LEASE_SECONDS = 300


def claim_due_alerts(db: Session, now: datetime) -> list[int]:
    """Marks every due alert as processing (with a lease) and returns their IDs."""
    due = db.query(models.Alert).filter(
        models.Alert.severity == "critical",
        models.Alert.status.in_(["new", "retrying", "processing"]),
        or_(models.Alert.next_attempt_at.is_(None), models.Alert.next_attempt_at <= now),
    ).all()

    for alert in due:
        if alert.status == "processing":
            logger.warning(f"[WATCHER] Alert {alert.id} lease expired mid-processing; picking it up again")
        alert.status = "processing"
        alert.attempts += 1
        alert.next_attempt_at = now + timedelta(seconds=LEASE_SECONDS)
    db.commit()
    return [alert.id for alert in due]


async def run_once(now: datetime | None = None) -> int:
    """One poll: claim due alerts and process them. Returns how many."""
    db: Session = SessionLocal()
    try:
        alert_ids = claim_due_alerts(db, now or datetime.now(timezone.utc))
    finally:
        db.close()

    if alert_ids:
        print(f"[WATCHER] Claimed {len(alert_ids)} due critical alert(s): {alert_ids}")
    for alert_id in alert_ids:
        # process_alert records every outcome itself (handled, retrying, or
        # left for the lease to expire), so one alert can't stop the others.
        await process_alert(alert_id)
    return len(alert_ids)


async def watch_critical_alerts():
    print("🔍 Alert Watcher started")  # use print as backup
    logger.info("🔍 Alert Watcher started")

    while True:
        try:
            await run_once()
        except Exception as e:
            # e.g. the database is unreachable: try again next poll.
            print(f"[WATCHER] Error: {e}")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def start_watcher_async():
    """Start the watcher task. Call from FastAPI async startup."""
    asyncio.create_task(watch_critical_alerts())


def start_watcher():
    """Sync wrapper for starting watcher in event loop. Use start_watcher_async() instead."""
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(watch_critical_alerts())
    except RuntimeError:
        # No running loop, schedule for when one starts
        asyncio.ensure_future(watch_critical_alerts())
