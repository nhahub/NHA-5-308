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


def get_hbase_table():
    conn = happybase.Connection("localhost", port=9090, timeout=10000)
    if TABLE_NAME.encode() not in conn.tables():
        conn.create_table(TABLE_NAME, {"d": dict()})
        print(f"Table '{TABLE_NAME}' created.")
    return conn, conn.table(TABLE_NAME)


# 1. Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="dual-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
)

# 2. HBase (hot storage)
hbase_connection, table = get_hbase_table()

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
            continue

        device_id = data.get("device_id", "UNKNOWN")
        timestamp = data.get("timestamp")
        row_key = f"{device_id}#{timestamp}"
        row_data = build_row(data)

        # Hot layer: HBase (with auto-reconnect)
        hbase_ok = False
        try:
            table.put(row_key.encode("utf-8"), row_data)
            hbase_ok = True
        except Exception:
            try:
                hbase_connection.close()
            except Exception:
                pass
            try:
                hbase_connection, table = get_hbase_table()
                table.put(row_key.encode("utf-8"), row_data)
                hbase_ok = True
            except Exception as e:
                print(f"HBase write failed for {row_key}: {e}")

        # Cold layer: MinIO (full message as JSON)
        minio_ok = False
        file_name = f"telemetry_data/{device_id}/{timestamp.replace(':', '-')}.json"
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json",
            )
            minio_ok = True
        except Exception as e:
            print(f"MinIO write failed for {row_key}: {e}")

        if hbase_ok or minio_ok:
            print(f"Saved -> HBase:{hbase_ok} | MinIO:{minio_ok} [{row_key}]")

except KeyboardInterrupt:
    print("\nDual consumer stopped by user.")
finally:
    consumer.close()
    try:
        hbase_connection.close()
    except Exception:
        pass