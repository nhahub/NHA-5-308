import json

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "fleet-telemetry",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="fleet-group-1",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
)

print("Listening to Kafka topic 'fleet-telemetry'...")

try:
    for message in consumer:
        data = message.value
        if not data:
            continue

        identity = data.get("identity", {}) or {}
        record = data.get("record", {}) or {}
        preview = dict(list(record.items())[:5])   # first 5 Excel columns

        print(
            f"Received -> Truck: {data.get('device_id')} | "
            f"Driver: {identity.get('driver_name')} | "
            f"{identity.get('origin')} -> {identity.get('destination')} | "
            f"{preview}"
        )
except KeyboardInterrupt:
    print("\nConsumer stopped.")
finally:
    consumer.close()