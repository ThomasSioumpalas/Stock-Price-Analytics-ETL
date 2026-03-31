import os
import json
from confluent_kafka import Consumer
from dotenv import load_dotenv
from databricks import sql

load_dotenv()

# kafka consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'market-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})
consumer.subscribe(['market-prices'])  # ← subscribe before polling

# databricks connection
connection = sql.connect(
    server_hostname=os.getenv("DATABRICKS_HOST"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN")
)
cursor = connection.cursor()

# create the raw table if it doesn't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS market_data.raw_market_prices (
        symbol STRING,
        timestamp STRING,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        ingested_at TIMESTAMP
    )
""")

# single polling loop
no_message_count = 0

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            no_message_count += 1
            if no_message_count > 10:
                print("No more messages, exiting.")
                break
            continue

        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        no_message_count = 0  # reset on valid message

        # parse the message
        row = json.loads(msg.value().decode('utf-8'))
        print(f"Consumed: {row}")

        # insert into Databricks
        cursor.execute("""
            INSERT INTO market_data.raw_market_prices
            VALUES (:symbol, :timestamp, :open, :high, :low, :close, :volume, current_timestamp())
        """, {
            'symbol': row['symbol'],
            'timestamp': row['timestamp'],
            'open': float(row['open']),
            'high': float(row['high']),
            'low': float(row['low']),
            'close': float(row['close']),
            'volume': int(row['volume'])
        })

        consumer.commit(msg)  # ← commit after successful insert

finally:
    consumer.close()
    cursor.close()
    connection.close()