import os, json, time, pathlib, requests
import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# --------------------------
# Config
# --------------------------
bootstrap = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
topic = os.getenv("TOPIC", "rides_raw_kafka")
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-07.parquet"
path = pathlib.Path("data/green_tripdata_2025-07.parquet")
path.parent.mkdir(exist_ok=True)

# --------------------------
# Download dataset jika belum ada
# --------------------------
if not path.exists():
    print("⬇️  Downloading dataset...")
    r = requests.get(url)
    r.raise_for_status()
    path.write_bytes(r.content)

# --------------------------
# Try create topic (safe fallback)
# --------------------------
try:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="taxi-producer")
    topic_list = [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
    admin.create_topics(new_topics=topic_list, validate_only=False)
    print(f"✅ Topic '{topic}' created")
except TopicAlreadyExistsError:
    print(f"ℹ️  Topic '{topic}' already exists")
except Exception as e:
    print(f"⚠️  Could not create topic via AdminClient: {e}")
    print("   → Assuming topic auto-creation is enabled on the broker.")

# --------------------------
# Kafka Producer
# --------------------------
producer = KafkaProducer(
    bootstrap_servers=bootstrap,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# --------------------------
# Read dataset
# --------------------------
df = pd.read_parquet(path)
print(f"📊 Loaded {len(df)} rows from parquet")

# --------------------------
# Send in chunks of 1000
# --------------------------
chunk_size = 1000
for start in range(0, len(df), chunk_size):
    chunk = df.iloc[start:start + chunk_size]
    print(f"🚀 Sending rows {start} → {start + len(chunk) - 1}")

    for _, row in chunk.iterrows():
        event = {
            "ride_id": f"{row.get('VendorID', '')}_{row.get('lpep_pickup_datetime', '')}",
            "pickup_ts": str(row.get("lpep_pickup_datetime")),
            "dropoff_ts": str(row.get("lpep_dropoff_datetime")),
            "pu_location": int(row.get("PULocationID", -1)),
            "do_location": int(row.get("DOLocationID", -1)),
            "fare": float(row.get("fare_amount", 0)),
            "tip": float(row.get("tip_amount", 0)),
            "payment_type": int(row.get("payment_type", -1)),
        }
        producer.send(topic, event)

    # Flush sekali per chunk
    producer.flush()
    print(f"✅ Sent {len(chunk)} messages")

    # Delay opsional supaya consumer tidak terlalu berat
    time.sleep(1)

print("🎉 Done sending all messages")
