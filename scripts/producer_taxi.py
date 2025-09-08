import requests, pathlib
import pandas as pd
import json, time, os
from kafka import KafkaProducer

# --- Download dataset jika belum ada ---
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-07.parquet"
path = pathlib.Path("data/green_tripdata_2025-07.parquet")
path.parent.mkdir(exist_ok=True)

if not path.exists():
    print("Downloading dataset...")
    r = requests.get(url)
    path.write_bytes(r.content)

# --- Kafka config ---
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
topic = os.getenv("TOPIC", "rides_raw")

# --- Load dataset ---
df = pd.read_parquet(path)

# --- Kirim ke Kafka per 1000 record ---
batch_size = 1000
events = []

for idx, row in df.iterrows():
    event = {
        "ride_id": str(row.get("VendorID", "")) + "_" + str(row.get("lpep_pickup_datetime", "")),
        "pickup_ts": str(row.get("lpep_pickup_datetime")),
        "dropoff_ts": str(row.get("lpep_dropoff_datetime")),
        "pu_location": int(row.get("PULocationID", -1)),
        "do_location": int(row.get("DOLocationID", -1)),
        "fare": float(row.get("fare_amount", 0)),
        "tip": float(row.get("tip_amount", 0)),
        "payment_type": int(row.get("payment_type", -1)),
    }
    events.append(event)

    # Jika batch sudah penuh, kirim semua
    if len(events) >= batch_size:
        for e in events:
            producer.send(topic, e)
        producer.flush()
        print(f"Sent {len(events)} messages up to index {idx}")
        events = []  # reset batch
        time.sleep(1)  # jeda antar batch (opsional)

# Kirim sisa data kalau ada
if events:
    for e in events:
        producer.send(topic, e)
    producer.flush()
    print(f"Sent remaining {len(events)} messages (final batch)")
