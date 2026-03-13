from datetime import datetime, timedelta
import json
import time
import requests
from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "crypto-prices"

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "ids": ",".join([
        "bitcoin", "ethereum", "solana", "cardano", "ripple",
        "dogecoin", "polkadot", "binancecoin", "avalanche", "chainlink",
        "polygon", "cosmos", "uniswap", "litecoin", "stellar",
        "vechain", "shiba-inu", "tron", "tezos", "neo",
    ]),
    "order": "market_cap_desc",
    "per_page": 20,
    "page": 1,
    "sparkline": "false",
    "price_change_percentage": "24h",
}

DESIRED_KEYS = [
    "id", "symbol", "current_price", "market_cap",
    "total_volume", "high_24h", "low_24h", "last_updated",
]


def fetch_and_push():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    response = requests.get(COINGECKO_URL, params=PARAMS)
    response.raise_for_status()

    data = response.json()
    filtered_data = [
        {key: coin.get(key) for key in DESIRED_KEYS}
        for coin in data
    ]

    payload = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "data": filtered_data,
    }

    producer.send(KAFKA_TOPIC, value=payload)
    producer.flush()
    producer.close()

    print(f"Pushed {len(filtered_data)} records at {payload['timestamp']}")


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="crypto_producer",
    default_args=default_args,
    description="Fetch crypto prices from CoinGecko and push to Kafka",
    schedule_interval="*/1 * * * *",
    start_date=datetime(2026, 3, 13),
    catchup=False,
    tags=["crypto", "kafka"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_and_push_to_kafka",
        python_callable=fetch_and_push,
    )