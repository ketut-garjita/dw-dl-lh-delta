import json, time, random, uuid, os
from datetime import datetime, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP","127.0.0.1:9092"),
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

from datetime import datetime, timedelta, timezone

def fake_event():
    now = datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, 1440))
    return {
        "ride_id": str(uuid.uuid4()),
        "pickup_ts": now.isoformat(),
        "dropoff_ts": (now + timedelta(minutes=random.randint(5, 60))).isoformat(),
        "pu_zone": random.choice(["Manhattan","Brooklyn","Queens"]),
        "do_zone": random.choice(["Manhattan","Brooklyn","Queens"]),
        "fare": round(random.uniform(4, 80), 2),
        "tip": round(random.uniform(0, 20), 2),
        "payment_type": random.choice(["card","cash","wallet"]),
    }

if __name__ == "__main__":
    topic = os.getenv("TOPIC","rides_raw")
    while True:
        producer.send(topic, fake_event())
        producer.flush()
        time.sleep(0.2)
