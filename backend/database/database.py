import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

load_dotenv()

# 1. Where the database lives.
# Reads DATABASE_URL from the environment (set by docker-compose.yml, or your
# local .env file). Falls back to a local SQLite file only if DATABASE_URL is
# not set, which is handy for quick local dev without a Postgres server running.
SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./sql_app.db")

# 2. Create the engine.
# check_same_thread=False is only needed/valid for SQLite; Postgres doesn't
# accept that connect arg, so it's only passed when we're actually on SQLite.
# On Postgres, pin the session timezone to UTC: TIMESTAMPTZ values are stored
# as UTC but returned in the session's timezone, which otherwise defaults to
# the server's local one (e.g. "+01:00" for Africa/Tunis). This keeps API
# timestamps in UTC regardless of where the database runs.
connect_args = (
    {"check_same_thread": False}
    if SQLALCHEMY_DATABASE_URL.startswith("sqlite")
    else {"options": "-c timezone=UTC"}
)
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args=connect_args)

# 3. Create a session factory
# This is what you will use to talk to the DB
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 4. Create the Base class
# Your models (Station, FuelData, Alert) will inherit from this
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()