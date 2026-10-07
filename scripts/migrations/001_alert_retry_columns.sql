-- Adds the alert-agent retry columns to an existing database.
--
-- Needed because tables are created with Base.metadata.create_all(), which
-- creates missing tables but never adds columns to existing ones. Fresh
-- databases get these columns automatically; existing ones need this once:
--
--   psql "$DATABASE_URL" -f scripts/migrations/001_alert_retry_columns.sql
--
-- Safe to run more than once.

ALTER TABLE alerts ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS last_error VARCHAR;
