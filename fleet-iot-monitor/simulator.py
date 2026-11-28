import json
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer

EXCEL_PATH = "Transportation and Logistics Tracking Dataset..xlsx"
SHEET_NAME = "Primary data"
TOPIC = "fleet-telemetry"
DELAY_SECONDS = 2      # pause between messages
LOOP = False           # True = replay the file again when it ends

df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
print("Columns:", df.columns.tolist())
print("Rows loaded:", len(df))


def clean(v):
    """Make any Excel/pandas value JSON-safe."""
    if pd.isna(v):
        return None
    if hasattr(v, "isoformat"):      # dates / timestamps / times
        return v.isoformat()
    if hasattr(v, "item"):           # numpy numbers -> python numbers
        return v.item()
    return v


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)

print("Starting Excel replay simulator...")

try:
    while True:
        for _, row in df.iterrows():
            record = {col: clean(val) for col, val in row.items()}

            message = {
                "device_id": str(record.get("vehicle_no")),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "identity": {
                    "fleet_id": record.get("customerNameCode"),
                    "driver_name": record.get("Driver_Name"),
                    "origin": record.get("Origin_Location"),
                    "destination": record.get("Destination_Location"),
                },
                # every column of the Excel row, exactly as it is in the file
                "record": record,
            }

            producer.send(TOPIC, value=message)
            producer.flush()
            print(f"Sent -> {message['device_id']} | {message['identity']['driver_name']}")
            time.sleep(DELAY_SECONDS)

        if not LOOP:
            break
    print("Finished sending all rows.")
except KeyboardInterrupt:
    print("\nSimulator stopped by user.")
finally:
    producer.close()