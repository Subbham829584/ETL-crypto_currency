import os
import json
import time
from datetime import datetime, timezone
from typing import Any
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(title="CryptoAnalytics API")

allowed_origins = [
    origin.strip()
    for origin in os.getenv("ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:8000").split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=int(os.getenv("POSTGRES_PORT", 5432)),
    dbname=os.getenv("POSTGRES_DB", "crypto_metrics"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD"),
)

if not DB["password"]:
    raise ValueError("POSTGRES_PASSWORD must be set for dashboard-api.")


def conn():
    return psycopg2.connect(**DB, cursor_factory=RealDictCursor)


def query(sql: str, params=()) -> list[dict[str, Any]]:
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

def query_one(sql: str, params=()) -> dict[str, Any]:
    rows = query(sql, params)
    return rows[0] if rows else {}

def _safe_pct(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return (numerator / denominator) * 100.0


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    db_ok = True
    try:
        with conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
    except Exception:
        db_ok = False

    freshness = query_one(
        """
        SELECT
            MAX(timestamp) AS last_data_at,
            CASE
                WHEN MAX(timestamp) IS NULL THEN NULL
                ELSE EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))
            END AS data_lag_seconds
        FROM crypto_table
        """
    )

    return {
        "status": "ok" if db_ok else "degraded",
        "db": "up" if db_ok else "down",
        "last_data_at": freshness.get("last_data_at"),
        "data_lag_seconds": freshness.get("data_lag_seconds"),
        "allowed_origins": allowed_origins,
    }


@app.get("/api/coins/latest")
def latest_prices():
    return query("""
        SELECT DISTINCT ON (id)
            timestamp, id, symbol, price, change_1min, change_5min, sma, ema, volatility
        FROM crypto_table
        ORDER BY id, timestamp DESC
    """)


@app.get("/api/coins")
def coin_list():
    return query("SELECT DISTINCT id, symbol FROM crypto_table ORDER BY id")


@app.get("/api/coins/{coin_id}/summary")
def coin_summary(coin_id: str, minutes: int = Query(60, ge=5, le=1440)):
    return query_one(
        """
        WITH base AS (
            SELECT timestamp, price, change_5min, volatility
            FROM crypto_table
            WHERE id = %s AND timestamp >= NOW() - (%s || ' minutes')::interval
        ),
        first_last AS (
            SELECT
                (SELECT price FROM base ORDER BY timestamp ASC LIMIT 1)  AS first_price,
                (SELECT price FROM base ORDER BY timestamp DESC LIMIT 1) AS last_price
        )
        SELECT
            %s AS coin_id,
            (SELECT COUNT(*) FROM base)                 AS points,
            MIN(price)                                  AS min_price,
            MAX(price)                                  AS max_price,
            ROUND(AVG(price)::numeric, 6)               AS avg_price,
            ROUND(AVG(change_5min)::numeric, 4)         AS avg_change_5min,
            ROUND(AVG(volatility)::numeric, 6)          AS avg_volatility,
            ROUND(
                CASE
                    WHEN (SELECT first_price FROM first_last) IS NULL
                         OR (SELECT first_price FROM first_last) = 0
                    THEN NULL
                    ELSE ((SELECT last_price FROM first_last) - (SELECT first_price FROM first_last))
                         / (SELECT first_price FROM first_last) * 100
                END::numeric, 4
            )                                           AS net_change_pct
        FROM base
        """,
        (coin_id, minutes, coin_id),
    )


@app.get("/api/coins/{coin_id}/history")
def coin_history(coin_id: str, minutes: int = Query(60, ge=5, le=1440)):
    return query("""
        SELECT timestamp, price, change_1min, change_5min, sma, ema, volatility
        FROM crypto_table
        WHERE id = %s AND timestamp >= NOW() - (%s || ' minutes')::interval
        ORDER BY timestamp ASC
    """, (coin_id, minutes))

@app.get("/api/coins/compare")
def compare_coins(ids: str = Query(..., description="Comma-separated ids, e.g. bitcoin,ethereum"), minutes: int = Query(180, ge=15, le=1440)):
    coin_ids = [c.strip().lower() for c in ids.split(",") if c.strip()]
    if len(coin_ids) < 2:
        return {"error": "Provide at least two coin ids in `ids` query parameter."}

    latest = query(
        """
        WITH ranked AS (
            SELECT
                id, symbol, timestamp, price, change_1min, change_5min, sma, ema, volatility,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC) AS rn
            FROM crypto_table
            WHERE id = ANY(%s)
        )
        SELECT id, symbol, timestamp, price, change_1min, change_5min, sma, ema, volatility
        FROM ranked
        WHERE rn = 1
        ORDER BY id
        """,
        (coin_ids,),
    )

    window_stats = query(
        """
        WITH base AS (
            SELECT id, symbol, timestamp, price, change_5min
            FROM crypto_table
            WHERE id = ANY(%s)
              AND timestamp >= NOW() - (%s || ' minutes')::interval
        ),
        first_last AS (
            SELECT
                id,
                MIN(timestamp) AS first_ts,
                MAX(timestamp) AS last_ts
            FROM base
            GROUP BY id
        ),
        first_last_price AS (
            SELECT
                fl.id,
                (SELECT price FROM base b WHERE b.id = fl.id AND b.timestamp = fl.first_ts LIMIT 1) AS first_price,
                (SELECT price FROM base b WHERE b.id = fl.id AND b.timestamp = fl.last_ts  LIMIT 1) AS last_price
            FROM first_last fl
        )
        SELECT
            b.id,
            MIN(b.symbol) AS symbol,
            COUNT(*) AS points,
            MIN(price) AS min_price,
            MAX(price) AS max_price,
            ROUND(AVG(price)::numeric, 6) AS avg_price,
            ROUND(AVG(change_5min)::numeric, 4) AS avg_change_5min,
            ROUND(
                CASE
                    WHEN flp.first_price IS NULL OR flp.first_price = 0 THEN NULL
                    ELSE ((flp.last_price - flp.first_price) / flp.first_price) * 100
                END::numeric, 4
            ) AS net_change_pct
        FROM base b
        LEFT JOIN first_last_price flp ON flp.id = b.id
        GROUP BY b.id, flp.first_price, flp.last_price
        ORDER BY b.id
        """,
        (coin_ids, minutes),
    )

    correlation = query_one(
        """
        WITH base AS (
            SELECT id, timestamp, price
            FROM crypto_table
            WHERE id = ANY(%s)
              AND timestamp >= NOW() - (%s || ' minutes')::interval
        ),
        pairs AS (
            SELECT
                b1.id AS id_a,
                b2.id AS id_b,
                b1.price AS price_a,
                b2.price AS price_b
            FROM base b1
            JOIN base b2
              ON b1.timestamp = b2.timestamp
             AND b1.id < b2.id
        )
        SELECT
            id_a,
            id_b,
            ROUND(CORR(price_a, price_b)::numeric, 4) AS corr,
            COUNT(*) AS overlap_points
        FROM pairs
        GROUP BY id_a, id_b
        ORDER BY overlap_points DESC
        LIMIT 1
        """,
        (coin_ids, minutes),
    )

    return {
        "window_minutes": minutes,
        "coin_ids": coin_ids,
        "latest": latest,
        "window_stats": window_stats,
        "top_pair_correlation": correlation,
    }


@app.get("/api/ohlcv/{coin_id}")
def ohlcv(coin_id: str, minutes: int = Query(60, ge=5, le=1440)):
    return query("""
        SELECT timestamp, open, high, low, close
        FROM ohlcv_1min
        WHERE id = %s AND timestamp >= NOW() - (%s || ' minutes')::interval
        ORDER BY timestamp ASC
    """, (coin_id, minutes))


@app.get("/api/gainers")
def gainers():
    return query("SELECT rank, id, symbol, price, change_5min FROM top_5_gainers ORDER BY rank")


@app.get("/api/losers")
def losers():
    return query("SELECT rank, id, symbol, price, change_5min FROM top_5_losers ORDER BY rank")


@app.get("/api/movers")
def movers(minutes: int = Query(5, ge=1, le=120), limit: int = Query(5, ge=1, le=50)):
    return {
        "window_minutes": minutes,
        "gainers": query(
            """
            WITH scoped AS (
                SELECT *
                FROM crypto_table
                WHERE timestamp >= NOW() - (%s || ' minutes')::interval
            ),
            ranked AS (
                SELECT
                    id,
                    symbol,
                    price,
                    change_5min,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC) AS rn
                FROM scoped
                WHERE change_5min IS NOT NULL
            )
            SELECT id, symbol, price, change_5min
            FROM ranked
            WHERE rn = 1
            ORDER BY change_5min DESC
            LIMIT %s
            """,
            (minutes, limit),
        ),
        "losers": query(
            """
            WITH scoped AS (
                SELECT *
                FROM crypto_table
                WHERE timestamp >= NOW() - (%s || ' minutes')::interval
            ),
            ranked AS (
                SELECT
                    id,
                    symbol,
                    price,
                    change_5min,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC) AS rn
                FROM scoped
                WHERE change_5min IS NOT NULL
            )
            SELECT id, symbol, price, change_5min
            FROM ranked
            WHERE rn = 1
            ORDER BY change_5min ASC
            LIMIT %s
            """,
            (minutes, limit),
        ),
    }

@app.get("/api/stream/market")
def stream_market(interval_seconds: int = Query(5, ge=2, le=60)):
    def event_stream():
        while True:
            payload = {
                "server_time": datetime.now(timezone.utc).isoformat(),
                "stats": query_one(
                    """
                    SELECT
                        (SELECT COUNT(DISTINCT id) FROM crypto_table) AS coins_tracked,
                        (SELECT MAX(timestamp) FROM crypto_table) AS last_updated
                    """
                ),
                "latest": query(
                    """
                    SELECT DISTINCT ON (id)
                        id, symbol, timestamp, price, change_1min, change_5min
                    FROM crypto_table
                    ORDER BY id, timestamp DESC
                    LIMIT 30
                    """
                ),
                "anomalies": query(
                    """
                    WITH recent AS (
                        SELECT id, symbol, timestamp, price, change_5min
                        FROM crypto_table
                        WHERE timestamp >= NOW() - INTERVAL '60 minutes'
                          AND change_5min IS NOT NULL
                    ),
                    stats AS (
                        SELECT id, AVG(change_5min) AS mean_change, STDDEV_POP(change_5min) AS std_change
                        FROM recent
                        GROUP BY id
                    )
                    SELECT r.id, r.symbol, r.timestamp, r.price, r.change_5min
                    FROM recent r
                    JOIN stats s ON s.id = r.id
                    WHERE s.std_change IS NOT NULL
                      AND s.std_change > 0
                      AND ABS((r.change_5min - s.mean_change) / s.std_change) >= 2.5
                    ORDER BY r.timestamp DESC
                    LIMIT 10
                    """
                ),
                "pipeline": query(
                    """
                    SELECT DISTINCT ON (task) task, status, records_pushed, latency_ms, created_at
                    FROM pipeline_metrics
                    ORDER BY task, created_at DESC
                    """
                ),
            }
            yield f"event: market_update\ndata: {json.dumps(payload, default=str)}\n\n"
            time.sleep(interval_seconds)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/alerts")
def alerts():
    return query("""
        SELECT alerted_at, coin_id, symbol, price, change_5min, alert_type
        FROM price_alerts
        ORDER BY alerted_at DESC
        LIMIT 50
    """)


@app.get("/api/pipeline")
def pipeline():
    return query("""
        SELECT DISTINCT ON (task)
            task, status, records_pushed, latency_ms, created_at
        FROM pipeline_metrics
        ORDER BY task, created_at DESC
    """)


@app.get("/api/pipeline/failures")
def pipeline_failures(limit: int = Query(20, ge=1, le=200)):
    return {
        "recent_failures": query(
            """
            SELECT run_id, task, status, latency_ms, error_message, created_at
            FROM pipeline_metrics
            WHERE status <> 'success'
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        ),
        "recent_dlq": query(
            """
            SELECT run_id, error_message, created_at
            FROM dead_letter_queue
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        ),
    }


@app.get("/api/anomalies")
def anomalies(minutes: int = Query(60, ge=10, le=1440), zscore: float = Query(2.0, ge=1.0, le=6.0)):
    return query(
        """
        WITH recent AS (
            SELECT id, symbol, timestamp, price, change_5min
            FROM crypto_table
            WHERE timestamp >= NOW() - (%s || ' minutes')::interval
              AND change_5min IS NOT NULL
        ),
        stats AS (
            SELECT
                id,
                AVG(change_5min) AS mean_change,
                STDDEV_POP(change_5min) AS std_change
            FROM recent
            GROUP BY id
        ),
        ranked AS (
            SELECT
                r.id,
                r.symbol,
                r.timestamp,
                r.price,
                r.change_5min,
                s.mean_change,
                s.std_change,
                CASE
                    WHEN s.std_change IS NULL OR s.std_change = 0 THEN NULL
                    ELSE (r.change_5min - s.mean_change) / s.std_change
                END AS z_score
            FROM recent r
            JOIN stats s ON s.id = r.id
        )
        SELECT id, symbol, timestamp, price, change_5min, ROUND(z_score::numeric, 4) AS z_score
        FROM ranked
        WHERE z_score IS NOT NULL AND ABS(z_score) >= %s
        ORDER BY ABS(z_score) DESC, timestamp DESC
        LIMIT 100
        """,
        (minutes, zscore),
    )

@app.get("/api/signals/backtest")
def backtest_signals(
    coin_id: str = Query(...),
    strategy: str = Query("ema_cross"),
    minutes: int = Query(1440, ge=60, le=10080),
    initial_capital: float = Query(1000.0, ge=100.0, le=1_000_000.0),
    fee_bps: float = Query(10.0, ge=0.0, le=200.0),
):
    if strategy != "ema_cross":
        return {"error": "Only strategy=ema_cross is currently supported."}

    rows = query(
        """
        SELECT timestamp, price, sma, ema
        FROM crypto_table
        WHERE id = %s
          AND timestamp >= NOW() - (%s || ' minutes')::interval
          AND price IS NOT NULL
        ORDER BY timestamp ASC
        """,
        (coin_id.lower(), minutes),
    )

    if len(rows) < 20:
        return {
            "coin_id": coin_id,
            "strategy": strategy,
            "error": "Not enough points for backtest (need at least 20).",
            "points": len(rows),
        }

    cash = initial_capital
    qty = 0.0
    in_position = False
    last_buy_price = None
    trades = []
    actions = []
    fee_rate = fee_bps / 10_000.0

    for row in rows:
        price = row.get("price")
        sma = row.get("sma")
        ema = row.get("ema")
        ts = row.get("timestamp")
        if price in (None, 0) or sma is None or ema is None:
            continue

        bullish = ema > sma
        bearish = ema < sma

        if bullish and not in_position and cash > 0:
            gross_qty = cash / price
            qty = gross_qty * (1.0 - fee_rate)
            cash = 0.0
            in_position = True
            last_buy_price = price
            actions.append({"time": ts, "action": "BUY", "price": price, "qty": qty})

        elif bearish and in_position and qty > 0:
            gross_cash = qty * price
            cash = gross_cash * (1.0 - fee_rate)
            pnl_pct = _safe_pct(price - (last_buy_price or price), last_buy_price or price)
            trades.append({
                "entry_price": last_buy_price,
                "exit_price": price,
                "pnl_pct": round(pnl_pct, 4) if pnl_pct is not None else None,
                "exit_time": ts,
            })
            qty = 0.0
            in_position = False
            last_buy_price = None
            actions.append({"time": ts, "action": "SELL", "price": price})

    last_price = rows[-1]["price"] if rows else None
    if in_position and qty > 0 and last_price:
        gross_cash = qty * last_price
        cash = gross_cash * (1.0 - fee_rate)
        pnl_pct = _safe_pct(last_price - (last_buy_price or last_price), last_buy_price or last_price)
        trades.append({
            "entry_price": last_buy_price,
            "exit_price": last_price,
            "pnl_pct": round(pnl_pct, 4) if pnl_pct is not None else None,
            "exit_time": rows[-1]["timestamp"],
        })
        actions.append({"time": rows[-1]["timestamp"], "action": "SELL_EOD", "price": last_price})

    final_equity = cash if cash > 0 else initial_capital
    strategy_return = _safe_pct(final_equity - initial_capital, initial_capital)
    first_price = rows[0]["price"]
    buy_hold_return = _safe_pct((last_price - first_price) if last_price and first_price else None, first_price)
    wins = sum(1 for t in trades if t.get("pnl_pct") is not None and t["pnl_pct"] > 0)
    win_rate = _safe_pct(wins, len(trades))

    return {
        "coin_id": coin_id.lower(),
        "strategy": strategy,
        "window_minutes": minutes,
        "points": len(rows),
        "initial_capital": round(initial_capital, 2),
        "final_equity": round(final_equity, 2),
        "strategy_return_pct": round(strategy_return, 4) if strategy_return is not None else None,
        "buy_hold_return_pct": round(buy_hold_return, 4) if buy_hold_return is not None else None,
        "excess_return_pct": round((strategy_return - buy_hold_return), 4) if strategy_return is not None and buy_hold_return is not None else None,
        "trades": len(trades),
        "win_rate_pct": round(win_rate, 2) if win_rate is not None else None,
        "recent_actions": actions[-10:],
    }

@app.get("/api/coins/{coin_id}/risk")
def coin_risk(coin_id: str, minutes: int = Query(240, ge=30, le=10080)):
    stats = query_one(
        """
        WITH scoped AS (
            SELECT id, timestamp, price, volatility, change_5min
            FROM crypto_table
            WHERE timestamp >= NOW() - (%s || ' minutes')::interval
        ),
        coin AS (
            SELECT * FROM scoped WHERE id = %s
        ),
        coin_drawdown AS (
            SELECT
                MIN((price / NULLIF(MAX(price) OVER (ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)) - 1) AS worst_drawdown
            FROM coin
        )
        SELECT
            (SELECT COUNT(*) FROM coin) AS points,
            (SELECT AVG(volatility) FROM coin) AS avg_volatility,
            (SELECT STDDEV_POP(change_5min) FROM coin) AS std_change_5min,
            (SELECT worst_drawdown FROM coin_drawdown LIMIT 1) AS worst_drawdown,
            (SELECT COUNT(*) FROM price_alerts WHERE coin_id = %s AND alerted_at >= NOW() - (%s || ' minutes')::interval) AS alerts_count,
            (
                WITH recent AS (
                    SELECT id, change_5min
                    FROM scoped
                    WHERE change_5min IS NOT NULL
                ),
                agg AS (
                    SELECT id, AVG(change_5min) AS m, STDDEV_POP(change_5min) AS s
                    FROM recent
                    GROUP BY id
                )
                SELECT COUNT(*)
                FROM crypto_table c
                JOIN agg a ON a.id = c.id
                WHERE c.id = %s
                  AND c.timestamp >= NOW() - (%s || ' minutes')::interval
                  AND c.change_5min IS NOT NULL
                  AND a.s IS NOT NULL
                  AND a.s > 0
                  AND ABS((c.change_5min - a.m) / a.s) >= 2.5
            ) AS anomaly_count
        """,
        (minutes, coin_id.lower(), coin_id.lower(), minutes, coin_id.lower(), minutes),
    )

    points = stats.get("points") or 0
    avg_vol = float(stats.get("avg_volatility") or 0.0)
    std_change = float(stats.get("std_change_5min") or 0.0)
    worst_drawdown = float(stats.get("worst_drawdown") or 0.0)
    alerts_count = int(stats.get("alerts_count") or 0)
    anomaly_count = int(stats.get("anomaly_count") or 0)

    vol_component = min(35.0, avg_vol * 25.0)
    move_component = min(25.0, std_change * 8.0)
    drawdown_component = min(25.0, abs(worst_drawdown) * 100.0 * 0.8)
    signal_component = min(15.0, (alerts_count * 0.7) + (anomaly_count * 1.2))
    risk_score = max(0.0, min(100.0, vol_component + move_component + drawdown_component + signal_component))

    band = "LOW"
    if risk_score >= 70:
        band = "HIGH"
    elif risk_score >= 40:
        band = "MEDIUM"

    return {
        "coin_id": coin_id.lower(),
        "window_minutes": minutes,
        "points": points,
        "risk_score": round(risk_score, 2),
        "risk_band": band,
        "components": {
            "avg_volatility": round(avg_vol, 6),
            "std_change_5min": round(std_change, 6),
            "worst_drawdown_pct": round(worst_drawdown * 100.0, 4),
            "alerts_count": alerts_count,
            "anomaly_count": anomaly_count,
        },
    }

@app.get("/api/data-quality")
def data_quality(minutes: int = Query(60, ge=10, le=10080)):
    summary = query_one(
        """
        WITH scoped AS (
            SELECT *
            FROM crypto_table
            WHERE timestamp >= NOW() - (%s || ' minutes')::interval
        ),
        latest_ts AS (
            SELECT MAX(timestamp) AS ts FROM crypto_table
        ),
        duplicates AS (
            SELECT COALESCE(SUM(cnt - 1), 0) AS dup_rows
            FROM (
                SELECT id, timestamp, COUNT(*) AS cnt
                FROM crypto_table
                GROUP BY id, timestamp
                HAVING COUNT(*) > 1
            ) d
        )
        SELECT
            (SELECT COUNT(*) FROM scoped) AS rows_window,
            (SELECT COUNT(DISTINCT id) FROM scoped) AS coins_window,
            (SELECT COUNT(*) FROM crypto_table) AS rows_total,
            (SELECT COUNT(DISTINCT id) FROM crypto_table) AS coins_total,
            (SELECT ts FROM latest_ts) AS last_data_at,
            CASE
                WHEN (SELECT ts FROM latest_ts) IS NULL THEN NULL
                ELSE EXTRACT(EPOCH FROM (NOW() - (SELECT ts FROM latest_ts)))
            END AS lag_seconds,
            ROUND(
                CASE WHEN (SELECT COUNT(*) FROM scoped) = 0 THEN 0
                     ELSE (SELECT COUNT(*) FROM scoped WHERE price IS NULL)::numeric / (SELECT COUNT(*) FROM scoped)::numeric * 100
                END, 4
            ) AS null_price_pct,
            ROUND(
                CASE WHEN (SELECT COUNT(*) FROM scoped) = 0 THEN 0
                     ELSE (SELECT COUNT(*) FROM scoped WHERE sma IS NULL OR ema IS NULL)::numeric / (SELECT COUNT(*) FROM scoped)::numeric * 100
                END, 4
            ) AS null_ma_pct,
            (SELECT dup_rows FROM duplicates) AS duplicate_rows_total
        """,
        (minutes,),
    )

    pipeline_q = query_one(
        """
        SELECT
            COUNT(*) FILTER (WHERE status = 'success') AS success_count,
            COUNT(*) FILTER (WHERE status <> 'success') AS failure_count
        FROM pipeline_metrics
        WHERE created_at >= NOW() - (%s || ' minutes')::interval
        """,
        (minutes,),
    )

    rows_window = int(summary.get("rows_window") or 0)
    lag_seconds = float(summary.get("lag_seconds") or 0.0) if summary.get("lag_seconds") is not None else None
    null_price = float(summary.get("null_price_pct") or 0.0)
    null_ma = float(summary.get("null_ma_pct") or 0.0)
    duplicates = int(summary.get("duplicate_rows_total") or 0)
    failure_count = int(pipeline_q.get("failure_count") or 0)

    score = 100.0
    if lag_seconds is not None:
        score -= min(35.0, lag_seconds / 60.0)
    score -= min(20.0, null_price * 1.2)
    score -= min(15.0, null_ma * 0.6)
    score -= min(20.0, duplicates * 0.05)
    score -= min(10.0, failure_count * 2.0)
    score = max(0.0, round(score, 2))

    status = "GOOD"
    if score < 70:
        status = "WARN"
    if score < 45:
        status = "CRITICAL"

    return {
        "window_minutes": minutes,
        "status": status,
        "quality_score": score,
        "freshness": {
            "last_data_at": summary.get("last_data_at"),
            "lag_seconds": lag_seconds,
        },
        "volume": {
            "rows_window": rows_window,
            "rows_total": int(summary.get("rows_total") or 0),
            "coins_window": int(summary.get("coins_window") or 0),
            "coins_total": int(summary.get("coins_total") or 0),
        },
        "completeness": {
            "null_price_pct": null_price,
            "null_ma_pct": null_ma,
            "duplicate_rows_total": duplicates,
        },
        "pipeline": {
            "success_count": int(pipeline_q.get("success_count") or 0),
            "failure_count": failure_count,
        },
    }


@app.get("/api/stats")
def stats():
    rows = query("""
        SELECT
            (SELECT COUNT(DISTINCT id) FROM crypto_table) AS coins_tracked,
            (SELECT MAX(timestamp) FROM crypto_table)     AS last_updated,
            (SELECT COUNT(*) FROM ohlcv_1min)             AS ohlcv_candles,
            (SELECT COUNT(*) FROM price_alerts)           AS total_alerts,
            (SELECT COUNT(*) FROM pipeline_metrics)       AS pipeline_events,
            (SELECT COUNT(*) FROM pipeline_metrics WHERE status = 'success') AS pipeline_successes,
            ROUND(
                (
                    SELECT
                        CASE WHEN COUNT(*) = 0 THEN NULL
                             ELSE COUNT(*) FILTER (WHERE status = 'success')::numeric / COUNT(*)::numeric * 100
                        END
                    FROM pipeline_metrics
                ), 2
            ) AS pipeline_success_rate
    """)
    return rows[0] if rows else {}
