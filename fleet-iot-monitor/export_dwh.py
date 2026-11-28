import io
import os
import boto3
import pandas as pd
from botocore.client import Config

ENDPOINT = "http://localhost:9000"
BUCKET = "fleet-bucket"
PREFIX = "dwh/"
OUT_DIR = "dwh_export"

TABLES = [
    "dim_vehicle",
    "dim_driver",
    "dim_trip",
    "dim_time",
    "fact_telemetry",
    "latest_status",
]

os.makedirs(OUT_DIR, exist_ok=True)

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

paginator = s3.get_paginator("list_objects_v2")

for table in TABLES:
    prefix = f"{PREFIX}{table}/"
    dfs = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
                dfs.append(pd.read_parquet(io.BytesIO(body)))

    if not dfs:
        print(f"\n[!] {table}: No parquet files found in MinIO ({prefix})")
        continue

    df = pd.concat(dfs, ignore_index=True)
    out_path = os.path.join(OUT_DIR, f"{table}.parquet")
    df.to_parquet(
        out_path,
        index=False,
        coerce_timestamps="ms",
        allow_truncated_timestamps=True,
    )

    print(f"\n=== {table} ({len(df):,} rows) -> {out_path} ===")
    for col, dtype in df.dtypes.items():
        sample = df[col].dropna().head(1).tolist()
        sample_val = sample[0] if sample else "NULL"
        print(f"  - {col} ({dtype}): {sample_val}")