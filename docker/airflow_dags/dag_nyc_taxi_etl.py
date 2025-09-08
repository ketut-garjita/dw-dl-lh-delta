from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, json, pathlib
from kafka import KafkaConsumer


MINIO_URL = os.getenv("MINIO_URL","http://minio:9000")
BUCKET = os.getenv("BRONZE_BUCKET","bronze")
ACCESS = os.getenv("MINIO_ACCESS_KEY","admin")
SECRET = os.getenv("MINIO_SECRET_KEY","password123")


# simple MinIO client via boto3
import boto3
s3 = boto3.client('s3', endpoint_url=MINIO_URL, aws_access_key_id=ACCESS, aws_secret_access_key=SECRET)


def land_from_kafka(**ctx):
    consumer = KafkaConsumer('rides_raw', bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP','kafka:9092'),
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest', enable_auto_commit=True)
    outdir = pathlib.Path('/tmp/bronze'); outdir.mkdir(parents=True, exist_ok=True)
    fname = outdir / f"rides_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    batch = []
for _ in range(1000):
    msg = next(consumer)
    batch.append(msg.value)
    fname.write_text("\n".join(json.dumps(x) for x in batch))
    s3.upload_file(str(fname), BUCKET, f"rides/date={datetime.utcnow().date()}/{fname.name}")


with DAG(
    dag_id="nyc_taxi_land_to_bronze",
    start_date=datetime(2024,1,1),
    schedule_interval="@hourly",
    catchup=False,
    default_args={"retries":1, "retry_delay": timedelta(minutes=5)},
):
PythonOperator(task_id="land_from_kafka", python_callable=land_from_kafka)
