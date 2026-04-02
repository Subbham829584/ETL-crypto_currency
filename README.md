<div align="center">

```
 ██████╗██████╗ ██╗   ██╗██████╗ ████████╗ ██████╗     ███████╗████████╗██╗
██╔════╝██╔══██╗╚██╗ ██╔╝██╔══██╗╚══██╔══╝██╔═══██╗    ██╔════╝╚══██╔══╝██║
██║     ██████╔╝ ╚████╔╝ ██████╔╝   ██║   ██║   ██║    █████╗     ██║   ██║
██║     ██╔══██╗  ╚██╔╝  ██╔═══╝    ██║   ██║   ██║    ██╔══╝     ██║   ██║
╚██████╗██║  ██║   ██║   ██║        ██║   ╚██████╔╝    ███████╗   ██║   ███████╗
 ╚═════╝╚═╝  ╚═╝   ╚═╝   ╚═╝        ╚═╝    ╚═════╝     ╚══════╝   ╚═╝   ╚══════╝
```

# Crypto Currency Real-Time ETL Pipeline

**A production-grade streaming data pipeline that ingests live cryptocurrency prices,<br>processes them with Apache Spark, and delivers analytics through a React dashboard.**

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10.4-017CEE?style=flat-square&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6.0-231F20?style=flat-square&logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C?style=flat-square&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-S3%20Compatible-C72E49?style=flat-square&logo=minio&logoColor=white)](https://min.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![React](https://img.shields.io/badge/React-Vite-61DAFB?style=flat-square&logo=react&logoColor=black)](https://react.dev/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docs.docker.com/compose/)

</div>

---

## 📖 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Data Flow](#-data-flow)
- [Services](#-services)
- [Pipeline DAGs](#-pipeline-dags)
- [Spark Jobs](#-spark-jobs)
- [Database Schema](#-database-schema)
- [Dashboard](#-dashboard)
- [API Reference](#-api-reference)
- [Getting Started](#-getting-started)
- [Configuration](#-configuration)
- [Monitoring & Observability](#-monitoring--observability)
- [Project Structure](#-project-structure)

---

## 🌐 Overview

**ETL-crypto_currency** is a fully containerised, real-time streaming pipeline that:

1. **Ingests** live prices for 20 cryptocurrencies from the CoinGecko API every minute (2 fetches × 30 s)
2. **Streams** raw payloads through Apache Kafka (`crypto-prices` topic)
3. **Persists** raw data to MinIO (S3-compatible object store) as partitioned Parquet files via Spark Structured Streaming
4. **Computes** advanced analytics — SMA, EMA, volatility, OHLCV 1-min candles, top gainers/losers, and PUMP/DUMP alerts — using a batch Spark job triggered by Airflow every 5 minutes
5. **Stores** computed results in PostgreSQL with idempotent upserts and a configurable retention window
6. **Visualises** everything on a live React dashboard (MarketFlow Atlas) that polls the FastAPI layer and auto-refreshes every 30 seconds

Every component has built-in data quality checks, dead-letter queuing, SLA monitoring, and email alerting.

---

## 🏗️ Architecture

```
                           ┌──────────────────────────────────────────────────────────────────┐
                           │                         Docker Network                            │
                           │                                                                   │
  ┌──────────────┐         │  ┌─────────────────────────────────────────────────────────────┐ │
  │  CoinGecko   │ ──API──▶│  │               Apache Airflow (LocalExecutor)                │ │
  │  REST API    │         │  │                                                             │ │
  └──────────────┘         │  │  DAG 1: Crypto_Producer (every 1 min)                      │ │
                           │  │  ┌─────────────┐  ┌──────────────────┐  ┌───────────────┐ │ │
                           │  │  │check_kafka  │─▶│fetch_and_push    │─▶│verify_kafka   │ │ │
                           │  │  │_health      │  │(2 × 30s batches) │  │_delivery      │ │ │
                           │  │  └─────────────┘  └──────────────────┘  └───────┬───────┘ │ │
                           │  │                                                  │         │ │
                           │  │                                          ┌───────▼───────┐ │ │
                           │  │                                          │log_run_summary│ │ │
                           │  │                                          └───────────────┘ │ │
                           │  │                                                             │ │
                           │  │  DAG 2: crypto_analytics (every 5 min)                     │ │
                           │  │  ┌──────────────────────────────────┐                      │ │
                           │  │  │  run_spark_analytics             │                      │ │
                           │  │  │  (touches .analytics_trigger)    │                      │ │
                           │  │  └──────────────────────────────────┘                      │ │
                           │  └─────────────────────────────────────────────────────────────┘ │
                           │                    │                   │                          │
                           │                    ▼                   ▼                          │
                           │  ┌──────────────────────┐   ┌──────────────────────┐             │
                           │  │   Apache Kafka        │   │   PostgreSQL (2x)    │             │
                           │  │   + ZooKeeper         │   │                      │             │
                           │  │                       │   │  ① App DB            │             │
                           │  │  Topic: crypto-prices │   │    crypto_table      │             │
                           │  │  Partitions: auto     │   │    ohlcv_1min        │             │
                           │  └──────────┬────────────┘   │    top_5_gainers     │             │
                           │             │                 │    top_5_losers      │             │
                           │             ▼                 │    price_alerts      │             │
                           │  ┌──────────────────────┐    │    pipeline_metrics  │             │
                           │  │   Spark Structured    │    │    dead_letter_queue │             │
                           │  │   Streaming           │    │                      │             │
                           │  │   (kafka_to_minio.py) │    │  ② Airflow metadata  │             │
                           │  │   trigger: 30s        │    └──────────┬───────────┘            │
                           │  └──────────┬────────────┘               │                       │
                           │             │ Parquet                     │                       │
                           │             ▼                             │                       │
                           │  ┌──────────────────────┐                │                       │
                           │  │       MinIO           │                │                       │
                           │  │   s3a://crypto-data/  │                │                       │
                           │  │   ├─ parquet/         │                │                       │
                           │  │   └─ checkpoints/     │                │                       │
                           │  └──────────┬────────────┘                │                       │
                           │             │                              │                       │
                           │             ▼                              │                       │
                           │  ┌──────────────────────┐                 │                       │
                           │  │   Spark Batch         │                 │                       │
                           │  │   (analytics.py)      │─────────────────┘                      │
                           │  │   SMA · EMA · OHLCV   │                                        │
                           │  │   Gainers · Alerts     │                                        │
                           │  └──────────────────────┘                                         │
                           │                                                                    │
                           │  ┌────────────────────────────────────────────────────────────┐   │
                           │  │                     Dashboard Layer                         │   │
                           │  │                                                             │   │
                           │  │   FastAPI (:8000)  ──────▶  React / Vite (:3000)           │   │
                           │  │   10 REST endpoints         MarketFlow Atlas UI             │   │
                           │  │   SSE live prices           Auto-refresh 30s               │   │
                           │  └────────────────────────────────────────────────────────────┘   │
                           └──────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Data Flow

```
CoinGecko API
     │
     │  JSON (20 coins × fields: id, symbol, price, market_cap,
     │         volume, high_24h, low_24h, last_updated)
     ▼
Airflow DAG: Crypto_Producer  (schedule: */1 * * * *)
     │
     ├─► Data quality checks
     │     • Is response a non-empty list?
     │     • Does each coin have id, symbol, current_price?
     │     • Is price > 0 and numeric?
     │     • Invalid records → dead_letter_queue (PostgreSQL)
     │
     ├─► Kafka Producer  →  topic: crypto-prices
     │     payload: { timestamp, data: [...valid_coins] }
     │     delivery verified by consumer offset check
     │
     └─► pipeline_metrics (PostgreSQL) — latency, records, status per task

Kafka topic: crypto-prices
     │
     ├─► Spark Structured Streaming  (trigger: 30s)
     │     parse JSON → explode coin array → flatten schema
     │     write Parquet to MinIO  s3a://crypto-data/parquet/
     │     checkpoint at          s3a://crypto-data/checkpoints/
     │
     └─► (available for any future consumer)

MinIO Parquet store
     │
     ▼
Airflow DAG: crypto_analytics  (schedule: */5 * * * *)
     │
     └─► touch .analytics_trigger
           │
           ▼
         Spark Batch Job  (analytics.py)
           │
           ├─ Read Parquet (last 10 min window)
           ├─ Compute per-coin window functions:
           │    price_1min_ago, price_5min_ago
           │    change_1min%, change_5min%
           │    SMA (5-period), EMA (3-period), volatility (stddev 5)
           ├─ Build 1-min OHLCV candles
           ├─ Rank top-5 gainers and losers (5-min change)
           ├─ Detect PUMP/DUMP alerts (|change_5min| ≥ threshold%)
           ├─ Upsert → crypto_table (ON CONFLICT DO UPDATE)
           ├─ Insert → ohlcv_1min (ON CONFLICT DO NOTHING)
           ├─ Truncate + insert → top_5_gainers, top_5_losers
           ├─ Append → price_alerts
           └─ Retention cleanup (configurable days, 5 tables)

PostgreSQL (App DB)
     │
     ▼
FastAPI  (:8000)  →  React / Nginx  (:3000)
```

---

## 🐳 Services

| Service | Image | Port(s) | Role |
|---------|-------|---------|------|
| `zookeeper` | `confluentinc/cp-zookeeper:7.6.0` | 2181 | Kafka coordination |
| `kafka` | `confluentinc/cp-kafka:7.6.0` | 9092 · 29092 | Message broker |
| `postgres` | `postgres:16-alpine` | **5435**→5432 | App database |
| `airflow-postgres` | `postgres:16-alpine` | — (internal) | Airflow metadata DB |
| `airflow-init` | custom (Airflow 2.10.4) | — | DB migrate + admin user |
| `airflow-webserver` | custom (Airflow 2.10.4) | **8080** | DAG UI |
| `airflow-scheduler` | custom (Airflow 2.10.4) | — | Trigger DAG runs |
| `minio` | `quay.io/minio/minio:latest` | **9010** (API) · **9011** (Console) | S3-compatible object store |
| `spark-master` | `apache/spark:3.5.1` | 7077 · **8090** | Spark cluster master |
| `spark-worker` | `apache/spark:3.5.1` | — | Worker (2 cores, 1 GB) |
| `spark-worker-2` | `apache/spark:3.5.1` | — | Worker (2 cores, 1 GB) |
| `spark-streaming` | `apache/spark:3.5.1` | — | Kafka → MinIO streaming |
| `spark-analytics` | `apache/spark:3.5.1` | — | Batch analytics (trigger-based) |
| `dashboard-api` | custom FastAPI | **8000** | REST API for dashboard |
| `dashboard-ui` | custom React/Nginx | **3000** | MarketFlow Atlas UI |

---

## 🔀 Pipeline DAGs

### DAG 1 — `Crypto_Producer` *(every minute)*

```
check_kafka_health → fetch_and_push_to_kafka → verify_kafka_delivery → log_run_summary
```

| Task | SLA | What it does |
|------|-----|-------------|
| `check_kafka_health` | 30 s | Connects via `KafkaAdminClient`, confirms topic exists |
| `fetch_and_push_to_kafka` | 50 s | Fetches CoinGecko × 2 (30 s apart), validates all fields, dead-letters invalids, pushes to Kafka, logs partition + offset |
| `verify_kafka_delivery` | 30 s | Creates a one-time consumer group, seeks to latest offset, confirms message is readable |
| `log_run_summary` | 10 s | XCom-pulls total pushed records, writes final row to `pipeline_metrics` |

**Resilience features:**
- 3 retries with 15 s delay
- Email on failure, retry, and success (configurable via `ALERT_EMAIL`)
- SLA miss callback for each task
- Per-batch dead-letter logging to PostgreSQL
- Kafka producer with 3-retry delivery guarantee

---

### DAG 2 — `crypto_analytics` *(every 5 minutes)*

```
run_spark_analytics
```

A single `BashOperator` that executes `submit_analytics.sh`, which touches a trigger file at `/opt/spark-jobs/.analytics_trigger`. The `spark-analytics` container polls this file every 10 seconds and fires `analytics.py` when found.

> This pattern decouples Airflow scheduling from Spark execution without requiring a Spark provider or REST hook.

---

## ⚡ Spark Jobs

### `kafka_to_minio.py` — Structured Streaming

- **Source:** Kafka topic `crypto-prices` (earliest offsets, fault-tolerant)
- **Parse:** `from_json` with full nested schema → `explode` coin array → flat row per coin
- **Sink:** MinIO Parquet, append mode, 30-second micro-batch trigger
- **Checkpoint:** MinIO `s3a://crypto-data/checkpoints/` for exactly-once semantics
- **Schema fields written:** `timestamp`, `id`, `symbol`, `price`, `market_cap`, `total_volume`, `high_24h`, `low_24h`, `last_updated`

### `analytics.py` — Batch Processing

Reads the last 10 minutes of Parquet data and computes:

| Metric | Method |
|--------|--------|
| `change_1min` | `lag(price, 1)` over per-coin time window |
| `change_5min` | `lag(price, 5)` over per-coin time window |
| `SMA` | `avg(price)` over rolling 5-row window |
| `EMA` | `avg(price)` over rolling 3-row window (approximation) |
| `volatility` | `stddev(price)` over rolling 5-row window |
| OHLCV candles | Bucket by `floor(unix_timestamp / 60) × 60`, agg open/high/low/close |
| Top-5 gainers | `row_number()` on `change_5min DESC`, top 5 per run |
| Top-5 losers | `row_number()` on `change_5min ASC`, top 5 per run |
| PUMP/DUMP alerts | `\|change_5min\| ≥ ALERT_THRESHOLD_PCT` (default: 2%) |

**Data retention:** Automatically deletes rows older than `RETENTION_DAYS` (default: 7) from all five app tables.

---

## 🗃️ Database Schema

```sql
-- Live analytics — one row per coin per timestamp, upserted
crypto_table (timestamp, id, symbol, price, change_1min, change_5min, sma, ema, volatility)
  UNIQUE INDEX (id, timestamp)

-- 1-minute OHLCV candles, closed candles only
ohlcv_1min (id, symbol, timestamp, open, high, low, close)
  UNIQUE INDEX (id, timestamp)

-- Snapshot tables, truncated + replaced every 5 min
top_5_gainers (rank, id, symbol, price, change_5min)
top_5_losers  (rank, id, symbol, price, change_5min)

-- Append-only alert log
price_alerts (id SERIAL, alerted_at, coin_id, symbol, price, change_5min, alert_type[PUMP|DUMP])

-- Observability
pipeline_metrics (id, run_id, task, status, records_pushed, latency_ms, error_message, created_at)
dead_letter_queue (id, run_id, payload, error_message, created_at)
```

---

## 🖥️ Dashboard

**MarketFlow Atlas** — a React + Vite + Tailwind CSS single-page app served via Nginx.

| Component | Data source | Description |
|-----------|-------------|-------------|
| **Header** | `/api/health`, `/api/stats`, `/api/coins/{id}/summary` | Pipeline status badge, data lag, 3-hour net change, avg volatility, anomaly count |
| **Live Ticker Board** | `/api/coins/latest` | Grid of all 20 coins — price, 1-min change, 5-min change, SMA, volatility; click to focus |
| **Price Chart** | `/api/coins/{id}/history` | Recharts line chart of historical price with SMA/EMA overlay for selected coin |
| **Top Movers** | `/api/gainers`, `/api/losers` | Side-by-side ranked lists of top-5 gainers and losers with 5-min % change |
| **Price Alerts Feed** | `/api/alerts` | Scrollable table of PUMP/DUMP events with timestamp, price, and change % |
| **Pipeline Health** | `/api/pipeline` | Task-level status cards showing latest status, latency, records pushed |

Auto-refreshes every **30 seconds**. All data is fetched from the FastAPI layer; the UI has no direct database access.

---

## 📡 API Reference

Base URL: `http://localhost:8000`

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | DB connectivity + data freshness (lag in seconds) |
| GET | `/api/coins/latest` | Latest row per coin: price, changes, SMA, EMA, volatility |
| GET | `/api/coins/{id}/history?minutes=N` | Time-series price history for one coin |
| GET | `/api/coins/{id}/summary?minutes=N` | Aggregate: open, close, high, low, net change %, avg volatility |
| GET | `/api/gainers` | Current top-5 gainers (5-min change) |
| GET | `/api/losers` | Current top-5 losers (5-min change) |
| GET | `/api/alerts?limit=N` | Latest price alerts (PUMP / DUMP) |
| GET | `/api/pipeline` | Latest status per Airflow task from `pipeline_metrics` |
| GET | `/api/stats` | Row counts per table + last updated timestamp |
| GET | `/api/anomalies?minutes=N&zscore=F` | Coins with Z-score above threshold in window |
| GET | `/api/prices/stream` | **SSE** — server-sent events of live price updates |

---

## 🚀 Getting Started

### Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- 8 GB RAM available to Docker (Spark needs headroom)
- CoinGecko API key (free demo key works; Pro key removes rate limits)

### 1. Clone & configure

```bash
git clone https://github.com/your-username/ETL-crypto_currency.git
cd ETL-crypto_currency
cp .env.example .env   # then edit .env
```

### 2. Fill in `.env`

```env
# ── CoinGecko ────────────────────────────────
API_KEY=your_coingecko_api_key
COINGECKO_API_HEADER=x-cg-demo-api-key   # or x-cg-pro-api-key for Pro

# ── PostgreSQL (App) ─────────────────────────
POSTGRES_USER=crypto
POSTGRES_PASSWORD=strongpassword
POSTGRES_DB=crypto_metrics

# ── PostgreSQL (Airflow) ─────────────────────
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflowpass
AIRFLOW_DB_NAME=airflow

# ── Airflow Admin ────────────────────────────
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com

# ── SMTP (for email alerts) ──────────────────
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your@gmail.com
SMTP_PASSWORD=your-app-password
ALERT_EMAIL=alerts@yourcompany.com

# ── MinIO ────────────────────────────────────
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# ── Spark / Storage ──────────────────────────
PARQUET_PATH=s3a://crypto-data/parquet/
CHECKPOINT_PATH=s3a://crypto-data/checkpoints/
RETENTION_DAYS=7
ALERT_THRESHOLD_PCT=2.0

# ── Kafka ────────────────────────────────────
KAFKA_BROKER=kafka:9092
KAFKA_TOPIC=crypto-prices
```

### 3. Start everything

```bash
docker compose up --build -d
```

> First startup takes 3–5 minutes as Spark downloads its JARs and Airflow migrates its DB.

### 4. MinIO bucket setup *(first run only)*

```bash
# Open MinIO Console → http://localhost:9011
# Login: minioadmin / minioadmin
# Create a bucket named: crypto-data
# Set it to public read (or keep private — Spark uses access key)
```

Or via CLI:

```bash
docker compose exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker compose exec minio mc mb local/crypto-data
```

### 5. Enable Airflow DAGs

```
http://localhost:8080  →  admin / admin
Toggle ON: Crypto_Producer
Toggle ON: crypto_analytics
```

### 6. Access services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Dashboard** | http://localhost:3000 | — |
| **API docs** | http://localhost:8000/docs | — |
| **Airflow** | http://localhost:8080 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| **MinIO Console** | http://localhost:9011 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| **Spark Master UI** | http://localhost:8090 | — |
| **PostgreSQL** | localhost:**5435** | `POSTGRES_USER` / `POSTGRES_PASSWORD` |

---

## ⚙️ Configuration

| Variable | Default | Effect |
|----------|---------|--------|
| `COINGECKO_IDS` | 20 coins (BTC, ETH, SOL…) | Comma-separated CoinGecko coin IDs to track |
| `EXPECTED_COIN_COUNT` | `20` | Minimum valid coins per API response (warns if below) |
| `ALERT_THRESHOLD_PCT` | `2.0` | % 5-min price move to trigger a PUMP/DUMP alert |
| `RETENTION_DAYS` | `7` | Days of data to keep across all tables |
| `PARQUET_PATH` | `s3a://crypto-data/parquet/` | MinIO destination for raw Parquet |
| `CHECKPOINT_PATH` | `s3a://crypto-data/checkpoints/` | Spark streaming checkpoint path |

---

## 📊 Monitoring & Observability

### Pipeline health at a glance

Every Airflow task writes a row to `pipeline_metrics`:

```sql
SELECT task, status, records_pushed, latency_ms, created_at
FROM pipeline_metrics
ORDER BY created_at DESC
LIMIT 20;
```

### Dead-letter inspection

Records that fail validation (missing price, invalid type, etc.) land in:

```sql
SELECT run_id, payload, error_message, created_at
FROM dead_letter_queue
ORDER BY created_at DESC;
```

### Active alerts

```sql
SELECT coin_id, symbol, price, change_5min, alert_type, alerted_at
FROM price_alerts
WHERE alerted_at > NOW() - INTERVAL '1 hour'
ORDER BY alerted_at DESC;
```

### Email alerts fire on:
- Any Airflow task failure (`email_on_failure: true`)
- Any Airflow task retry (`email_on_retry: true`)
- Successful DAG completion (`on_success_callback`)
- SLA miss on any task (`sla_miss_callback`)

---

## 📁 Project Structure

```
ETL-crypto_currency-main/
│
├── airflow/
│   ├── Dockerfile              # Airflow 2.10.4 + Java JDK + PySpark
│   └── requirements.txt        # kafka-python, psycopg2, pyspark, airflow-spark
│
├── dags/
│   ├── crypto_producer_dag.py  # Main producer DAG (Kafka health → fetch → verify → log)
│   └── crypto_analytics_dag.py # Analytics trigger DAG (touch trigger file)
│
├── spark-jobs/
│   ├── kafka_to_minio.py       # Structured streaming: Kafka → Parquet → MinIO
│   ├── analytics.py            # Batch analytics: MinIO → SMA/EMA/OHLCV → PostgreSQL
│   └── submit_analytics.sh     # Trigger file creator (called by Airflow)
│
├── dashboard/
│   ├── api/
│   │   ├── main.py             # FastAPI: 10 REST endpoints + SSE stream
│   │   ├── requirements.txt    # fastapi, psycopg2, uvicorn
│   │   └── Dockerfile
│   └── ui/
│       ├── src/
│       │   ├── App.jsx                     # Root layout + data fetching hooks
│       │   └── components/
│       │       ├── CoinGrid.jsx            # Live ticker board (20 coins)
│       │       ├── PriceChart.jsx          # Recharts price + SMA/EMA
│       │       ├── TopMovers.jsx           # Gainers / losers lists
│       │       ├── AlertsFeed.jsx          # PUMP/DUMP alert table
│       │       └── PipelineStatus.jsx      # Task health cards
│       ├── Dockerfile                      # Vite build → Nginx serve
│       ├── nginx.conf
│       └── package.json                    # React, Axios, Recharts, Tailwind
│
├── postgres/
│   └── init.sql                # Schema + indexes for all 7 app tables
│
├── docker-compose.yml          # Full 15-service stack definition
└── .gitignore
```

---

## 🏗️ Design Decisions

**Why trigger-file pattern for Spark?**
The `spark-analytics` container polls a trigger file rather than using the Spark REST API or Airflow's Spark provider. This avoids firewall/auth complexity between Airflow and Spark, keeps the container always-warm (no cold-start JVM), and lets the Airflow task complete fast while Spark runs async.

**Why two PostgreSQL instances?**
Airflow's metadata DB and the application DB are intentionally separate — schema conflicts, backup strategies, and access patterns are different. Using a single instance risks Airflow migrations breaking app queries.

**Why MinIO as an intermediate store?**
Landing raw data as Parquet in object storage (MinIO = self-hosted S3) before computing analytics gives you: replay capability (re-run analytics on old data), schema evolution, compression (Parquet ~5–10× smaller than raw JSON), and the ability to plug in additional consumers (ML training, reporting) without touching the pipeline.

**Why upsert instead of append for `crypto_table`?**
`ON CONFLICT (id, timestamp) DO UPDATE` means the analytics job is idempotent — re-running it on the same Parquet window won't create duplicate rows. This is critical since `spark-analytics` retries on failure.

---

<div align="center">

Built with Apache Airflow · Kafka · Spark · MinIO · PostgreSQL · FastAPI · React

</div>
