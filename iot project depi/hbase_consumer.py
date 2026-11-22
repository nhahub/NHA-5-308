import json
import re

import happybase
from kafka import KafkaConsumer

TOPIC = "fleet-telemetry"
TABLE_NAME = "fleet_telemetry"

# Excel columns already stored from "identity", so we don't store them twice
SKIP_IN_RECORD = {
    "vehicle_no",
    "customerNameCode",
    "Driver_Name",
    "Origin_Location",
    "Destination_Location",
}


def clean_name(name):
    """HBase-friendly column name (letters, digits, underscore)."""
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


# 1. HBase connection (Thrift, port 9090) + create table if missing
connection = happybase.Connection("localhost", port=9090)
if TABLE_NAME.encode() not in connection.tables():
    connection.create_table(TABLE_NAME, {"d": dict()})
    print(f"Table '{TABLE_NAME}' created.")
table = connection.table(TABLE_NAME)

# 2. Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="hbase-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
)

print("Listening to Kafka and saving to HBase...")

try:
    for message in consumer:
        data = message.value
        if not data or not data.get("record"):
            continue   # skip old-format messages

        device_id = data.get("device_id")
        timestamp = data.get("timestamp")
        row_key = f"{device_id}#{timestamp}"

        try:
            table.put(row_key.encode("utf-8"), build_row(data))
            print(f"Saved to HBase -> RowKey: {row_key}")
        except Exception as e:
            print(f"HBase write failed for {row_key}: {e}")
except KeyboardInterrupt:
    print("\nHBase consumer stopped.")
finally:
    consumer.close()
    connection.close()