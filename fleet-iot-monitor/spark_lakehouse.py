import re

import happybase
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

BASE = "s3a://fleet-bucket/"
DWH = BASE + "dwh/"

# Columns already represented in the dimensions (not repeated in the fact table)
RESERVED = {
    "vehicle_no", "event_time", "fleet_id", "driver_name", "origin",
    "destination", "time_minute",
    "customerNameCode", "Driver_Name", "Origin_Location", "Destination_Location",
}


def clean_name(name):
    """Parquet-safe column name."""
    return re.sub(r"[^0-9a-zA-Z_]+", "_", str(name)).strip("_")


# 1. Spark session + MinIO (S3A)
# NOTE: the hadoop-aws version must match your PySpark's Hadoop version.
spark = (
    SparkSession.builder
    .appName("Fleet_DWH_Pipeline")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.5.0")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("Spark session connected to MinIO")

# 2. Cold layer: raw JSON from MinIO  (telemetry_data/<device_id>/<timestamp>.json)
raw = spark.read.json(BASE + "telemetry_data/*/*.json")
raw = raw.filter(F.col("record").isNotNull())      # skip old-format files
rec_cols = raw.select("record.*").columns
extra_cols = [c for c in rec_cols if clean_name(c) not in RESERVED]
extra_clean = [clean_name(c) for c in extra_cols]
print("Excel columns found:", rec_cols)

df = raw.select(
    F.col("device_id").alias("vehicle_no"),
    F.to_timestamp("timestamp").alias("event_time"),
    F.col("identity.fleet_id").alias("fleet_id"),
    F.col("identity.driver_name").alias("driver_name"),
    F.col("identity.origin").alias("origin"),
    F.col("identity.destination").alias("destination"),
    *[F.col(f"record.`{c}`").alias(clean_name(c)) for c in extra_cols],
).dropna(subset=["vehicle_no", "event_time"])

df = df.fillna({"origin": "Unknown", "destination": "Unknown"})
df = df.withColumn("time_minute", F.date_trunc("minute", "event_time"))

# 3. Dimensions
dim_vehicle = (
    df.select("vehicle_no", "fleet_id").dropDuplicates(["vehicle_no"])
    .withColumn("vehicle_key", F.row_number().over(Window.orderBy("vehicle_no")))
)

dim_trip = (
    df.select("origin", "destination").distinct()
    .withColumn("trip_key", F.row_number().over(Window.orderBy("origin", "destination")))
)

dim_time = (
    df.select(F.col("time_minute").alias("full_date")).distinct()
    .withColumn("time_key", F.row_number().over(Window.orderBy("full_date")))
    .withColumn("year", F.year("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("hour", F.hour("full_date"))
    .withColumn("minute", F.minute("full_date"))
)

# 4. Fact table: all remaining Excel columns + keys
fact_telemetry = (
    df.join(dim_vehicle.select("vehicle_no", "vehicle_key"), "vehicle_no")
    .join(dim_trip.select("origin", "destination", "trip_key"), ["origin", "destination"])
    .join(
        dim_time.select(F.col("full_date").alias("time_minute"), "time_key"),
        "time_minute",
    )
    .select(
        F.monotonically_increasing_id().alias("telemetry_key"),
        "vehicle_key", "trip_key", "time_key", "event_time",
        *extra_clean,
    )
)

# 5. Hot layer: latest reading per truck from HBase (via happybase)
conn = happybase.Connection("localhost", port=9090)
rows = []
for key, data in conn.table("fleet_telemetry").scan():
    vehicle_no, ts = key.decode().split("#", 1)
    row = {"vehicle_no": vehicle_no, "event_time": ts}
    for col, val in data.items():
        row[col.decode().split(":", 1)[1]] = val.decode()
    rows.append(row)
conn.close()

latest_status = None
if rows:
    all_cols = sorted({k for r in rows for k in r})
    schema = StructType([StructField(c, StringType(), True) for c in all_cols])
    hb_df = spark.createDataFrame(
        [tuple(r.get(c) for c in all_cols) for r in rows], schema
    ).withColumn("event_time", F.to_timestamp("event_time"))
    w_latest = Window.partitionBy("vehicle_no").orderBy(F.col("event_time").desc())
    latest_status = (
        hb_df.withColumn("rn", F.row_number().over(w_latest))
        .filter("rn = 1").drop("rn")
    )

# 6. Write the DWH layer to MinIO as Parquet
print("Writing DWH tables to MinIO (Parquet)...")
dim_vehicle.write.mode("overwrite").parquet(DWH + "dim_vehicle")
dim_trip.write.mode("overwrite").parquet(DWH + "dim_trip")
dim_time.write.mode("overwrite").parquet(DWH + "dim_time")
fact_telemetry.write.mode("overwrite").parquet(DWH + "fact_telemetry")
if latest_status is not None:
    latest_status.write.mode("overwrite").parquet(DWH + "latest_status")
    latest_status.show(5)

print("Fact rows:", fact_telemetry.count())
fact_telemetry.show(5)
print("Done: DWH built from MinIO (history) and HBase (latest status)")