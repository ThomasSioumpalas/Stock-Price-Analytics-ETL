import os
import json
import time
import requests
from dotenv import load_dotenv
from confluent_kafka import Producer
from datetime import datetime, timedelta


load_dotenv()

ALPHA_VANTAGE_API = os.getenv("ALPHA_VANTAGE_API")
TOPIC = "market-prices"

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered → topic={msg.topic()} offset={msg.offset()}")

def fetch_and_produce():
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo'
    r = requests.get(url)
    data = r.json()

    if "Time Series (Daily)" not in data:
        print("API error:", data)
        return

    time_series = data["Time Series (Daily)"]

    cutoff = datetime.now() - timedelta(days=15)
    
    for timestamp, prices in time_series.items():
        
        date = datetime.strptime(timestamp, "%Y-%m-%d")
        if date < cutoff:
            continue  
    
    
        message = {
            "symbol": "IBM",
            "timestamp": timestamp,
            "open": prices["1. open"],
            "high": prices["2. high"],
            "low": prices["3. low"],
            "close": prices["4. close"],
            "volume": prices["5. volume"]
        }
        producer.produce(
            topic=TOPIC,
            key=timestamp,
            value=json.dumps(message),
            callback=delivery_report
        )

    producer.flush()
    print(f"Batch produced successfully")


fetch_and_produce()