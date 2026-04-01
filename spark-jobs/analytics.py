import sys
import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, from_unixtime, avg, stddev, lag, round, when,
    row_number, current_timestamp, expr, min as spark_min, max as spark_max,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .appName("CryptoAnalytics") \
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000")) \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

DB_CONN = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DB", "crypto_metrics"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD"),
)

PARQUET_PATH = os.getenv("PARQUET_PATH", "s3a://crypto-data/parquet/")
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "7"))
ALERT_THRESHOLD_PCT = float(os.getenv("ALERT_THRESHOLD_PCT", "2.0"))

if not os.getenv("MINIO_ACCESS_KEY") or not os.getenv("MINIO_SECRET_KEY"):
    raise ValueError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set.")

if not DB_CONN["password"]:
    raise ValueError("POSTGRES_PASSWORD must be set.")


def pg_conn():
    return psycopg2.connect(**DB_CONN)


try:
    df = spark.read.parquet(PARQUET_PATH).drop(
        "market_cap", "total_volume", "high_24h", "low_24h", "last_updated"
    )
    df = df.withColumn("timestamp", to_timestamp("timestamp")) \
           .withColumn("price", col("price").cast(DoubleType()))

    latest_data = df.filter(
        col("timestamp") >= current_timestamp() - expr("INTERVAL 10 MINUTES")
    )
    if latest_data.count() == 0:
        print("No data found in the last 10 minutes. Exiting.")
        spark.stop()    
        sys.exit(0)

    coin_window = Window.partitionBy("id").orderBy("timestamp")

    # ── Price changes ────────────────────────────────────────────────────
    latest_data = latest_data \
        .withColumn("price_1min_ago", lag("price", 1).over(coin_window)) \
        .withColumn("price_5min_ago", lag("price", 5).over(coin_window)) \
        .withColumn(
            "change_1min",
            when(col("price_1min_ago").isNull(), None)
            .otherwise(round((col("price") - col("price_1min_ago")) / col("price_1min_ago") * 100, 2)),
        ) \
        .withColumn(
            "change_5min",
            when(col("price_5min_ago").isNull(), None)
            .otherwise(round((col("price") - col("price_5min_ago")) / col("price_5min_ago") * 100, 2)),
        ) \
        .drop("price_1min_ago", "price_5min_ago")

    # ── SMA, EMA, Volatility ─────────────────────────────────────────────
    latest_data = latest_data \
        .withColumn("SMA", avg("price").over(coin_window.rowsBetween(-4, 0))) \
        .withColumn("EMA", avg("price").over(coin_window.rowsBetween(-2, 0))) \
        .withColumn("volatility", stddev("price").over(coin_window.rowsBetween(-4, 0)))

    # ── Latest record per coin (one row per coin — no cross-run duplicates) ──
    latest_per_coin = latest_data \
        .withColumn("rn", row_number().over(
            Window.partitionBy("id").orderBy(col("timestamp").desc())
        )) \
        .filter(col("rn") == 1) \
        .drop("rn")

    # ── OHLCV 1-minute candles ───────────────────────────────────────────
    ohlcv_df = df.filter(
        col("timestamp") >= current_timestamp() - expr("INTERVAL 10 MINUTES")
    ).withColumn(
        "minute_bucket",
        to_timestamp(from_unixtime(expr("FLOOR(unix_timestamp(timestamp) / 60) * 60")))
    ).groupBy("id", "symbol", "minute_bucket").agg(
        expr("min_by(price, timestamp)").alias("open"),
        spark_max("price").alias("high"),
        spark_min("price").alias("low"),
        expr("max_by(price, timestamp)").alias("close"),
    ).withColumnRenamed("minute_bucket", "timestamp") \
     .filter(col("timestamp") < current_timestamp() - expr("INTERVAL 1 MINUTES"))

    # ── Top 5 gainers & losers (with price + change) ─────────────────────
    gain_df = latest_per_coin \
        .filter(col("change_5min").isNotNull()) \
        .withColumn("rank", row_number().over(Window.orderBy(col("change_5min").desc()))) \
        .filter(col("rank") <= 5) \
        .select("rank", "id", "symbol", "price", "change_5min")

    loss_df = latest_per_coin \
        .filter(col("change_5min").isNotNull()) \
        .withColumn("rank", row_number().over(Window.orderBy(col("change_5min").asc()))) \
        .filter(col("rank") <= 5) \
        .select("rank", "id", "symbol", "price", "change_5min")

    # ── Price alerts (>ALERT_THRESHOLD_PCT move in 5 min) ────────────────
    alerts_df = latest_per_coin \
        .filter(
            col("change_5min").isNotNull() & (
                (col("change_5min") >= ALERT_THRESHOLD_PCT) |
                (col("change_5min") <= -ALERT_THRESHOLD_PCT)
            )
        ) \
        .select(
            current_timestamp().alias("alerted_at"),
            "id", "symbol", "price", "change_5min",
            when(col("change_5min") >= ALERT_THRESHOLD_PCT, "PUMP").otherwise("DUMP").alias("alert_type"),
        )

    # ── Console output ───────────────────────────────────────────────────
    print("\n=== Latest Metrics per Coin ===")
    latest_per_coin.select(
        "timestamp", "id", "symbol", "price",
        "change_1min", "change_5min", "SMA", "EMA", "volatility",
    ).orderBy("id").show(truncate=False)

    print("=== 1-Min OHLCV Candles ===")
    ohlcv_df.orderBy("id", "timestamp").show(truncate=False)

    print("=== Top 5 Gainers ===")
    gain_df.show(truncate=False)

    print("=== Top 5 Losers ===")
    loss_df.show(truncate=False)

    print("=== Price Alerts ===")
    alerts_df.show(truncate=False)

    # ── Write to PostgreSQL ──────────────────────────────────────────────
    # crypto_table: upsert (latest per coin, update if same timestamp reprocessed)
    metrics_rows = latest_per_coin.select(
        "timestamp", "id", "symbol", "price",
        "change_1min", "change_5min", "SMA", "EMA", "volatility",
    ).collect()

    conn = pg_conn()
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO crypto_table (timestamp, id, symbol, price, change_1min, change_5min, sma, ema, volatility)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id, timestamp) DO UPDATE SET
            price       = EXCLUDED.price,
            change_1min = EXCLUDED.change_1min,
            change_5min = EXCLUDED.change_5min,
            sma         = EXCLUDED.sma,
            ema         = EXCLUDED.ema,
            volatility  = EXCLUDED.volatility
        """,
        [(r.timestamp, r.id, r.symbol, r.price,
          r.change_1min, r.change_5min, r.SMA, r.EMA, r.volatility)
         for r in metrics_rows],
    )

    # ohlcv_1min: insert closed candles, skip duplicates
    ohlcv_rows = ohlcv_df.collect()
    if ohlcv_rows:
        cur.executemany(
            """
            INSERT INTO ohlcv_1min (id, symbol, timestamp, open, high, low, close)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id, timestamp) DO NOTHING
            """,
            [(r.id, r.symbol, r.timestamp, r.open, r.high, r.low, r.close)
             for r in ohlcv_rows],
        )

    # top_5 tables: truncate + insert (always fresh snapshot)
    cur.execute("TRUNCATE TABLE top_5_gainers")
    cur.executemany(
        "INSERT INTO top_5_gainers (rank, id, symbol, price, change_5min) VALUES (%s, %s, %s, %s, %s)",
        [(r.rank, r.id, r.symbol, r.price, r.change_5min) for r in gain_df.collect()],
    )
    cur.execute("TRUNCATE TABLE top_5_losers")
    cur.executemany(
        "INSERT INTO top_5_losers (rank, id, symbol, price, change_5min) VALUES (%s, %s, %s, %s, %s)",
        [(r.rank, r.id, r.symbol, r.price, r.change_5min) for r in loss_df.collect()],
    )

    # price_alerts: append
    alert_rows = alerts_df.collect()
    if alert_rows:
        cur.executemany(
            "INSERT INTO price_alerts (alerted_at, coin_id, symbol, price, change_5min, alert_type) VALUES (%s,%s,%s,%s,%s,%s)",
            [(r.alerted_at, r.id, r.symbol, r.price, r.change_5min, r.alert_type)
             for r in alert_rows],
        )

    conn.commit()

    # ── Data retention cleanup ───────────────────────────────────────────
    for table, ts_col in [
        ("crypto_table",     "timestamp"),
        ("ohlcv_1min",       "timestamp"),
        ("price_alerts",     "alerted_at"),
        ("pipeline_metrics", "created_at"),
        ("dead_letter_queue","created_at"),
    ]:
        cur.execute(
            f"DELETE FROM {table} WHERE {ts_col} < NOW() - INTERVAL %s",
            (f"{RETENTION_DAYS} days",)
        )
        if cur.rowcount:
            print(f"Retention: removed {cur.rowcount} old rows from {table}")

    conn.commit()
    cur.close()
    conn.close()

    print("\nAll data written to PostgreSQL successfully.")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
