import json
import re

import boto3
import happybase
from botocore.client import Config
from kafka import KafkaConsumer

TOPIC = "fleet-telemetry"
TABLE_NAME = "fleet_telemetry"
BUCKET_NAME = "fleet-bucket"

SKIP_IN_RECORD = {
    "vehicle_no",
    "customerNameCode",
    "Driver_Name",
    "Origin_Location",
    "Destination_Location",
}


def clean_name(name):
    return re.sub(r"[^0-9a-zA-Z_]+", "_", str(name)).strip("_")


def to_bytes(value):
    return ("" if value is None else str(value)).encode("utf-8")


def build_row(data):
    identity = data.get("identity", {}) or {}
    record = data.get("record", {}) or {}

    row = {
        b"d:fleet_id": to_bytes(identity.get("fleet_id")),
        b"d:driver_name": to_bytes(identity.get("driver_name")),
        b"d:origin": to_bytes(identity.get("origin")),
        b"d:destination": to_bytes(identity.get("destination")),
    }
    for col, val in record.items():
        if col in SKIP_IN_RECORD:
            continue
        row[f"d:{clean_name(col)}".encode("utf-8")] = to_bytes(val)
    return row


# 1. Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="dual-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
)

# 2. HBase (hot storage) + create table if missing
hbase_connection = happybase.Connection("localhost", port=9090)
if TABLE_NAME.encode() not in hbase_connection.tables():
    hbase_connection.create_table(TABLE_NAME, {"d": dict()})
    print(f"Table '{TABLE_NAME}' created.")
table = hbase_connection.table(TABLE_NAME)

# 3. MinIO (data lake, S3 API)
s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

try:
    s3_client.head_bucket(Bucket=BUCKET_NAME)
except Exception:
    s3_client.create_bucket(Bucket=BUCKET_NAME)

print("Dual consumer (HBase + MinIO) is running and listening to Kafka...")

try:
    for message in consumer:
        data = message.value
        if not data or not data.get("record"):
            continue   # skip old-format messages

        device_id = data.get("device_id", "UNKNOWN")
        timestamp = data.get("timestamp")
        row_key = f"{device_id}#{timestamp}"

        try:
            # Hot layer: HBase
            table.put(row_key.encode("utf-8"), build_row(data))

            # Cold layer: MinIO (full message as JSON)
            file_name = f"telemetry_data/{device_id}/{timestamp.replace(':', '-')}.json"
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json",
            )

            print(f"Saved -> HBase [{row_key}] & MinIO [{file_name}]")
        except Exception as e:
            print(f"Write failed for {row_key}: {e}")
except KeyboardInterrupt:
    print("\nDual consumer stopped by user.")
finally:
    consumer.close()
    hbase_connection.close()