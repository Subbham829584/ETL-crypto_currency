-- Crypto data tables
CREATE TABLE IF NOT EXISTS crypto_table (
    timestamp   TIMESTAMP,
    id          TEXT,
    symbol      TEXT,
    price       DOUBLE PRECISION,
    change_1min DOUBLE PRECISION,
    change_5min DOUBLE PRECISION,
    sma         DOUBLE PRECISION,
    ema         DOUBLE PRECISION,
    volatility  DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS top_5_gainers (
    rank   INTEGER NOT NULL,
    id     TEXT,
    symbol TEXT
);

CREATE TABLE IF NOT EXISTS top_5_losers (
    rank   INTEGER NOT NULL,
    id     TEXT,
    symbol TEXT
);

-- Pipeline monitoring tables
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id              SERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL,
    task            TEXT NOT NULL,
    status          TEXT NOT NULL,
    records_pushed  INTEGER DEFAULT 0,
    latency_ms      INTEGER DEFAULT 0,
    error_message   TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id              SERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL,
    payload         TEXT,
    error_message   TEXT NOT NULL,
    created_at      TIMESTAMP DEFAULT NOW()
);