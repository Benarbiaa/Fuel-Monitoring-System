# Fuel Station Monitoring System

A full-stack fuel monitoring and forecasting platform for AGIL fuel stations. Collects real-time telemetry, generates automated alerts, forecasts stock levels, runs an autonomous response agent, and provides an AI-powered chat assistant with semantic search over past reports.

> **Reviewing this project?** [`docs/demo-script.md`](docs/demo-script.md) walks through the Kafka ingestion pipeline live in ~5 minutes, [`docs/debugging/timestamp-timezone-bug.md`](docs/debugging/timestamp-timezone-bug.md) is a full writeup of a real bug found and fixed during development — how it was investigated, where the investigation initially went wrong, and why — and [`docs/debugging/kafka-reliability-review.md`](docs/debugging/kafka-reliability-review.md) covers what happened to readings when parts of the pipeline failed, how each problem was reproduced, and how it's now tested.

---

## Architecture

Ingestion is event-driven, backed by Kafka; everything else is independent HTTP services + one simulation agent:

```
Fuel-Monitorin-System/
├── backend/               FastAPI REST API + automation agent      → port 8000
├── ingestion_consumer/    Kafka consumer, sole writer to Postgres
├── kafka_common/          Shared Kafka producer wrapper
├── chat/                  AI chat microservice                     → port 8001
├── frontend/               Angular 16 dashboard                    → port 4200
└── stations/              Station simulation agent (Kafka producer)
```

**Ingestion pipeline:** station telemetry (from the simulator, or from `POST /ingest`) is published to a Kafka topic (`fuel.readings.raw`), keyed by `station_id:fuel_type` to preserve per-tank ordering. A dedicated consumer service validates each message, writes it to Postgres idempotently (unique constraint on `station_id` + `fuel_type` + `timestamp`, safe under Kafka's at-least-once delivery), and generates alerts. `/ingest` itself no longer writes to the database — it's a thin producer that returns `202 Accepted` once Kafka has confirmed it stored the reading (`503` if it can't, so the client knows to retry). A reading and its alerts are stored in one database transaction, and the Kafka offset is committed only after that. A message that fails is retried in place with backoff, preserving per-tank order; database outages are retried until the database is back, while messages that are malformed or keep failing are routed to a dead-letter topic (`fuel.readings.dlq`) instead of blocking the pipeline. See [`docs/adr/0001-kafka-ingestion.md`](docs/adr/0001-kafka-ingestion.md) for the full rationale, alternatives considered, and known local-dev gotchas.

This was a deliberate migration from an earlier version where `/ingest` wrote synchronously to the database in the same request — see the ADR for why that was limiting (no replay, no backpressure isolation, ingestion coupled to DB write throughput).

The backend and chat services are fully independent processes that only talk to each other over HTTP — the chat service never imports backend code directly.

---

## Features

- **Kafka-backed ingestion** — replayable event log, idempotent producer and consumer, retries with backoff, dead-letter handling for malformed or repeatedly failing messages
- **Real-time telemetry** — stations push fuel readings every 10 minutes, auto-registering new stations on first contact
- **Automated alerts** — LOW_STOCK, PRICE_ANOMALY, HIGH_CONSUMPTION, STATION_CRITICAL, RESTOCK (delivery detection, inferred from stock-level increases)
- **Autonomous response agent** — background watcher polls for critical alerts and uses an LLM to decide/execute an action (reorder, notify manager, or escalate), with retries and backoff, a human-escalation fallback, and an audit trail of every action taken
- **Stock forecasting** — Prophet-based time-series predictions with LLM narrative
- **AI chat assistant** — Groq-powered agent with tool-calling over live stock/alerts/forecast data, plus semantic search over historical reports
- **LLM-generated reports** — persisted station reports (Markdown + PDF export), searchable by the chat assistant for pattern/history questions
- **Multi-station dashboard** — Angular SPA with live stock, history, alerts, forecast, and chat tabs

---

## Prerequisites

- **Python 3.11 or 3.12** (avoid 3.14 — several dependencies, including `pydantic-core` and `prophet`'s stack, don't yet ship prebuilt wheels for it and will fail to build from source)
- Node.js 18+ and npm
- PostgreSQL 14+

---

## Docker (recommended)

The fastest way to run the full stack — all services share one `DATABASE_URL` wired correctly to the bundled Postgres container.

**Prerequisites:** Docker Desktop

```bash
# 1. Copy and fill in your credentials
cp .env.example .env
# Edit .env — set POSTGRES_PASSWORD and GROQ_API_KEY at minimum

# 2. Build and start all services
docker compose up --build
```

| Service | URL |
|---|---|
| Dashboard | http://localhost |
| Backend API docs | http://localhost:8000/docs |
| Chat service docs | http://localhost:8001/docs |

```bash
# Stop everything
docker compose down

# Stop and delete all data (DB + vector store)
docker compose down -v
```

---

## Manual Setup (running locally without Docker)

### 1. Python environment

```bash
python3.11 -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
```

### 2. Backend dependencies

```bash
pip install -r requirements.txt
```

### 3. Frontend dependencies

```bash
cd frontend
npm install
```

### 4. Chat service dependencies

```bash
cd chat
pip install -r requirements.txt
```

The first time the chat assistant searches past reports, `chromadb` downloads a small (~80MB) embedding model automatically — this needs normal internet access and only happens once (cached afterward).

### 5. PostgreSQL setup

Start your local Postgres server, then create the database once:

```bash
createdb fuelmonitor
```

### 6. Environment variables

**Project root** — copy the template and fill in your values:

```bash
cp .env.example .env
```

```env
DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@localhost:5432/fuelmonitor
GROQ_API_KEY=your_groq_api_key_here
SMTP_USER=your_email@gmail.com
SMTP_PASS=your_app_password
MANAGER_EMAIL=manager@example.com
KAFKA_BOOTSTRAP_SERVERS=localhost:9092   # optional, this is the default
```

Get a free Groq API key at [console.groq.com](https://console.groq.com).

**`chat/.env`** — the chat microservice runs as a separate process with its own environment file (no `.env.example` provided for it, create it manually):

```env
GROQ_API_KEY=your_groq_api_key_here
BACKEND_MODE=real
BACKEND_URL=http://localhost:8000
SERVICE_PORT=8001
```

Set `BACKEND_MODE=mock` instead if you want to test the chat UI without running the backend at all — it'll use `chat/mocks/backend_mock.py` for fake data.

### 7. Initialize the database

Creates the tables (run once; also happens automatically on backend startup, but useful for a clean first-time setup):

```bash
python init_db.py
```

**Upgrading an existing database?** `create_all()` never adds columns to existing tables, so apply the scripts in `scripts/migrations/` once, in order:

```bash
psql "$DATABASE_URL" -f scripts/migrations/001_alert_retry_columns.sql
```

---

## Running

Start each service in a separate terminal:

**Terminal 1 — Kafka (required for ingestion)**
```bash
./scripts/start-kafka.sh
```
Starts a single-broker Kafka container (KRaft mode) and creates the `fuel.readings.raw` / `fuel.readings.dlq` topics if they don't already exist. See [`docs/adr/0001-kafka-ingestion.md`](docs/adr/0001-kafka-ingestion.md) if you need to understand or troubleshoot this.

**Terminal 2 — Backend API**
```bash
uvicorn backend.main:app --reload --port 8000
```
Look for `🔍 Alert Watcher started` in the logs — confirms the automation agent is running alongside the API.

**Terminal 3 — Ingestion consumer (required — this is what actually writes readings to the database)**
```bash
python -m ingestion_consumer.main
```
`/ingest` and the station agent both only publish to Kafka; nothing is persisted until this consumer processes it.

**Terminal 4 — Chat microservice**
```bash
cd chat
uvicorn main:app --reload --port 8001
```

**Terminal 5 — Frontend**
```bash
cd frontend
npm start
```

**Terminal 6 — Station agent (optional, feeds live test data every 10 minutes)**
```bash
cd stations
python agilAgentStation.py --sink kafka
```
`--sink http` is also available, which routes through `POST /ingest` instead of publishing to Kafka directly — useful for testing the HTTP path specifically, though since `/ingest` itself now just publishes to Kafka too (see Architecture), this adds an extra hop rather than bypassing Kafka entirely.

| Service | URL |
|---|---|
| Dashboard | http://localhost:4200 |
| Backend API docs | http://localhost:8000/docs |
| Chat service docs | http://localhost:8001/docs |

---

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

The suite covers the ingestion pipeline — alert rules, the consumer (transactions, retries, ordering, DLQ routing), the producer and `POST /ingest` — and the automation agent (retries, fallback escalation, leases, audit-trail consistency), with the LLM faked so no API calls are made. It runs against an in-memory SQLite database with small Kafka fakes, so it needs no services running. One integration test also runs against a real broker when one is available (`./scripts/start-kafka.sh`), and is skipped otherwise.

---

## Alert Thresholds

| Alert | Condition | Severity |
|---|---|---|
| LOW_STOCK | Stock < 15% of capacity | warning |
| LOW_STOCK | Stock < 5% of capacity | critical |
| PRICE_ANOMALY | Price deviates > 5% from official | warning |
| PRICE_ANOMALY | Price deviates > 10% from official | critical |
| HIGH_CONSUMPTION | Sales > 200 L in 5 minutes | warning |
| STATION_CRITICAL | Stock reaches 0 L | critical |

---

## Automation Agent

A background watcher in the backend process polls every 30 seconds for `critical` alerts, hands each one to an LLM (`responder.py`) to decide an action — `reorder`, `notify_manager`, or `escalate` — and executes it via `actions.py` (email/log). Every action taken is recorded in the `incident_logs` table, committed together with the alert's new status so the audit trail can't disagree with it.

Every outcome ends in a defined state — a critical alert is never closed without an action:

| What happens | Alert ends up |
|---|---|
| LLM decides, action succeeds | `acknowledged`, `handled_by="agent"` |
| LLM times out / is rate-limited / errors, or the action fails (e.g. mail server down) | `retrying`, with the error in `last_error`; retried after 30s, 1m, 2m, 4m |
| Still failing after 5 attempts | escalated to a human: `acknowledged`, `handled_by="fallback"` |
| LLM unusable (no API key, key rejected) | escalated immediately, `handled_by="fallback"` |
| LLM picks an action that doesn't exist | escalated instead (LLM output is never trusted as-is) |
| Backend dies mid-processing | picked up again when its 5-minute lease expires |

See [`docs/debugging/alert-agent-review.md`](docs/debugging/alert-agent-review.md) for how each of these was broken before, and how it's tested.

---

## Project Structure

```
backend/
├── main.py                  FastAPI app, startup, and automation-agent watcher launch
├── schemas.py                Pydantic request/response models
├── database/
│   ├── models.py             SQLAlchemy ORM models (Station, FuelData, Alert, IncidentLog, Report)
│   └── database.py           DB engine — reads DATABASE_URL, falls back to SQLite for quick local dev
├── routes/
│   ├── ingest.py              POST /ingest
│   ├── data.py                GET /stations /companies /current /history /alerts
│   ├── prophet_routes.py      GET /predict
│   └── report_routes.py       GET /report /report/pdf /reports (list history)
├── services/
│   ├── storage.py             DB read/write helpers, auto-registers new stations on ingest
│   ├── alerts.py              Alert rules applied to each incoming reading (used by the consumer)
│   ├── prophet_service.py     Stock forecasting
│   └── report_services.py     LLM report generation
└── agent/
    ├── watcher.py              Polls DB for critical alerts (status="new")
    ├── responder.py            LLM decision-making for alerts
    └── actions.py               Executes alert actions (email, reorder, escalate) + incident log

chat/
├── main.py                    FastAPI app entry point
├── config.py                  Settings from environment
├── schemas.py                 Chat request/response models
├── routes/
│   ├── chat.py                 POST /chat
│   └── alerts.py                GET /alerts (with LLM enrichment)
├── services/
│   ├── gemini_service.py        Groq LLM tool-calling loop
│   ├── agent_tools.py           Tool implementations (stock, alerts, forecast, report search)
│   ├── report_retriever.py      ChromaDB semantic search over persisted reports
│   └── alert_enricher.py        Async LLM alert explanations
└── mocks/
    └── backend_mock.py          Mock data for offline development (BACKEND_MODE=mock)

frontend/src/app/
├── core/
│   ├── models/                 TypeScript interfaces
│   └── services/                HTTP services for all API endpoints
├── shared/                     Reusable components and pipes
└── features/
    ├── single-station/          Tabbed station view (overview, history, alerts, forecast, chat, report)
    └── multi-station/            Fleet overview with station cards and global alerts

stations/
└── agilAgentStation.py          Simulates a station sending live telemetry

ingestion_consumer/
└── main.py                      Kafka consumer: validate, store + alert in one transaction, retry, DLQ

kafka_common/
└── readings_producer.py         Shared idempotent producer (simulator + /ingest)

tests/                           pytest suite for the ingestion pipeline
```

---

## AI / Chat Assistant Tools

The chat agent (`chat/services/gemini_service.py`) has access to these tools, dispatched via `chat/services/agent_tools.py`:

| Tool | Purpose |
|---|---|
| `get_current_stock` | Live stock/price for one or all stations |
| `get_fuel_history` | Historical fuel data for trends |
| `get_alerts` | Structured alert lookup (filterable by station/severity/type) |
| `get_station_count` | Total stations in the network |
| `get_lowest_stock` | Stations ranked by lowest stock % |
| `get_station_summary` | Full snapshot of one station |
| `get_critical_alerts` | Urgent alerts across the network |
| `predict_stock` | Prophet-based forecast for a station/fuel type |
| `search_past_reports` | Semantic search over previously generated reports — for pattern/history questions that don't map to a structured filter |

`search_past_reports` re-indexes from the backend's `/reports` endpoint on every call rather than relying on a separate background sync job — this keeps it self-healing and always fresh at the cost of a small amount of repeated work per search.

---

## Known Limitations / Things to Revisit

- **Schema changes require a full table drop/recreate** — the project uses `Base.metadata.create_all()`, which only creates missing tables and never alters existing ones. Two schema changes during the Kafka migration (a unique constraint, and switching timestamp columns to `timezone=True`) both required manually dropping and recreating tables, and the alert-agent retry columns needed a hand-written script (`scripts/migrations/`). This is fine for a solo dev project with disposable data, but is exactly the gap Alembic migrations exist to close — worth adding before this schema changes again.
- **`/report` makes a live, uncached Groq API call every time it's hit** — repeated requests (e.g. tab switches) each trigger a fresh LLM call. Worth caching by station_id with a short TTL if cost/latency becomes a concern.
- **Kafka runs as a single broker with `replication.factor=1` everywhere**, including its internal topics — correct and necessary for a one-node local dev setup, but not representative of how a production cluster would be configured (see the ADR's "Known gotcha" section for why this specific setting matters).
- Frontend has known `npm audit` findings inherited from the Angular 16 dependency tree (54 vulnerabilities at last check, mostly transitive). Not urgent, but worth revisiting during a future Angular upgrade.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Kafka (KRaft mode), confluent-kafka-python |
| Backend | Python, FastAPI, SQLAlchemy, PostgreSQL |
| Forecasting | Prophet, pandas |
| AI / LLM | Groq (`openai/gpt-oss-120b`, `openai/gpt-oss-20b`) |
| RAG | ChromaDB + sentence-transformers, over persisted LLM-generated reports |
| Reports | ReportLab, Markdown |
| Frontend | Angular 16, Chart.js, ngx-markdown |
| Communication | Kafka (ingestion) + REST over HTTP (everything else) |