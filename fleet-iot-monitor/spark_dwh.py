import os
import re
import sys

# Tell Spark which Python to use (fixes the python3 problem on Windows)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Hadoop / winutils paths (must be set before Spark starts)
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

import happybase
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

BASE = "s3a://fleet-bucket/"
DWH = BASE + "dwh/"
# Test first with ONE truck folder, e.g. BASE + "telemetry_data/HR47C3126/*.json"
SOURCE = BASE + "telemetry_data/*/*.json"

# Never copied into the DWH (personal data)
PII = {"Driver_MobileNo"}

# Columns that live in dimensions, not in the fact table
IN_DIMS = {
    "vehicle_no", "customerNameCode", "Driver_Name",
    "Origin_Location", "Destination_Location",
    "Material Shipped", "Data_Ping_time",
}


def clean_name(name):
    """Parquet-safe column name."""
    return re.sub(r"[^0-9a-zA-Z_]+", "_", str(name)).strip("_")


# 1. Spark session + MinIO (S3A)
os.makedirs("C:\\spark-temp", exist_ok=True)

spark = (
    SparkSession.builder
    .appName("Fleet_DWH_Pipeline_v2")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.local.dir", "C:\\spark-temp")
    .config("spark.hadoop.fs.s3a.buffer.dir", "C:/spark-temp/s3a")
    .config("spark.hadoop.hadoop.tmp.dir", "C:/spark-temp/hadoop")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
    .config("spark.sql.sources.parallelPartitionDiscovery.threshold", "100000")
    .config("spark.sql.files.ignoreMissingFiles", "true")
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print("Spark session connected to MinIO")

# 2. Cold layer: raw JSON from MinIO
print("Reading JSON from MinIO (thousands of small files can take a few minutes)...")
raw = spark.read.format("json").load(SOURCE)
raw = raw.filter(F.col("record").isNotNull())      # skip old-format files
rec_cols = raw.select("record.*").columns
print("Excel columns found:", rec_cols)


def rec(name):
    """record.<name> as string, or NULL if the Excel has no such column."""
    if name in rec_cols:
        return F.col(f"record.`{name}`").cast("string")
    return F.lit(None).cast("string")


def num(name):
    """record.<name> as double (bad values become NULL instead of failing)."""
    if name in rec_cols:
        return F.expr(f"try_cast(`record`.`{name}` AS double)")
    return F.lit(None).cast("double")


# The real event time comes from the Excel (Data_Ping_time).
# If it cannot be parsed, fall back to the replay time of the simulator.
ping_time = (
    F.expr("try_to_timestamp(cast(`record`.`Data_Ping_time` AS string))")
    if "Data_Ping_time" in rec_cols else F.lit(None).cast("timestamp")
)
replay_time = F.to_timestamp("timestamp")

other_cols = [
    c for c in rec_cols
    if c not in IN_DIMS and c not in PII
    and c not in {"Curr_lat", "Curr_lon", "BookingID", "delay", "ontime",
                  "TRANSPORTATION_DISTANCE_IN_KM"}
]

df = raw.select(
    F.col("device_id").alias("vehicle_no"),
    F.coalesce(ping_time, replay_time).alias("event_time"),
    replay_time.alias("ingest_time"),
    F.col("identity.fleet_id").alias("fleet_id"),
    F.col("identity.driver_name").alias("driver_name"),
    F.col("identity.origin").alias("origin"),
    F.col("identity.destination").alias("destination"),
    rec("BookingID").alias("BookingID"),
    F.initcap(F.trim(rec("Material Shipped"))).alias("material"),
    num("Curr_lat").alias("lat"),
    num("Curr_lon").alias("lon"),
    num("TRANSPORTATION_DISTANCE_IN_KM").alias("distance_km"),
    rec("delay").alias("delay"),
    rec("ontime").alias("ontime"),
    *[rec(c).alias(clean_name(c)) for c in other_cols],
).dropna(subset=["vehicle_no", "event_time"])

df = df.fillna({"origin": "Unknown", "destination": "Unknown",
                "driver_name": "Unknown", "material": "Unknown"})

# Readable delay status:  R = delayed, G = on time  (assumption - confirm with your data owner)
df = df.withColumn(
    "delay_status",
    F.when(F.col("delay") == "R", "Delayed")
     .when(F.col("ontime") == "G", "On time")
     .otherwise("Unknown"),
)

# Remove duplicates (same truck + booking + ping time)
df = df.dropDuplicates(["vehicle_no", "BookingID", "event_time"])
df = df.withColumn("time_hour", F.date_trunc("hour", "event_time"))
df = df.cache()
print("Rows loaded from MinIO:", df.count())


# 3. Dimensions (stable hash keys, no global window needed)
def hkey(*cols):
    return F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols])


dim_vehicle = (
    df.select("vehicle_no", "fleet_id").dropDuplicates(["vehicle_no"])
    .withColumn("vehicle_key", hkey("vehicle_no"))
)

dim_driver = (
    df.select("driver_name").distinct()
    .withColumn("driver_key", hkey("driver_name"))
)

dim_trip = (
    df.select("BookingID", "origin", "destination", "material", "distance_km")
    .dropDuplicates(["BookingID"])
    .withColumn("trip_key", hkey("BookingID"))
)

dim_time = (
    df.select(F.col("time_hour").alias("full_date")).distinct()
    .withColumn("time_key", F.date_format("full_date", "yyyyMMddHH").cast("long"))
    .withColumn("year", F.year("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("hour", F.hour("full_date"))
)

# 4. Fact table (slim: keys + measurements, no repeated text)
fact_telemetry = df.select(
    hkey("vehicle_no", "BookingID", "event_time").alias("telemetry_key"),
    hkey("vehicle_no").alias("vehicle_key"),
    hkey("driver_name").alias("driver_key"),
    hkey("BookingID").alias("trip_key"),
    F.date_format("time_hour", "yyyyMMddHH").cast("long").alias("time_key"),
    "BookingID", "event_time", "ingest_time",
    "lat", "lon", "distance_km", "delay_status",
    *[clean_name(c) for c in other_cols],
)

# 5. Hot layer: latest reading per truck from HBase (new-format rows only)
latest_status = None
try:
    conn = happybase.Connection("localhost", port=9090, timeout=5000)
    rows = []
    for key, data in conn.table("fleet_telemetry").scan():
        if b"d:BookingID" not in data:        # skip legacy random-simulator rows
            continue
        vehicle_no, ts = key.decode().split("#", 1)
        row = {"vehicle_no": vehicle_no, "event_time": ts}
        for col, val in data.items():
            name = col.decode().split(":", 1)[1]
            if name in {clean_name(p) for p in PII}:
                continue
            row[name] = val.decode()
        rows.append(row)
    conn.close()

    if rows:
        all_cols = sorted({k for r in rows for k in r})
        schema = StructType([StructField(c, StringType(), True) for c in all_cols])
        hb_df = (
            spark.createDataFrame(
                [tuple(r.get(c) for c in all_cols) for r in rows], schema
            )
            .withColumn("event_time", F.to_timestamp("event_time"))
            .dropna(subset=["vehicle_no", "event_time"])
        )
        max_ts_df = hb_df.groupBy("vehicle_no").agg(
            F.max("event_time").alias("event_time")
        )
        latest_status = (
            hb_df.join(max_ts_df, on=["vehicle_no", "event_time"], how="inner")
            .dropDuplicates(["vehicle_no"])
        )
except Exception as e:
    print("HBase step skipped:", str(e).strip().splitlines()[0] if str(e).strip() else e)

# 6. Write the DWH layer to MinIO as Parquet
print("Writing DWH tables to MinIO (Parquet)...")
dim_vehicle.coalesce(1).write.mode("overwrite").parquet(DWH + "dim_vehicle")
dim_driver.coalesce(1).write.mode("overwrite").parquet(DWH + "dim_driver")
dim_trip.coalesce(1).write.mode("overwrite").parquet(DWH + "dim_trip")
dim_time.coalesce(1).write.mode("overwrite").parquet(DWH + "dim_time")
fact_telemetry.coalesce(4).write.mode("overwrite").parquet(DWH + "fact_telemetry")

if latest_status is not None:
    try:
        latest_status.coalesce(1).write.mode("overwrite").parquet(DWH + "latest_status")
    except Exception as e:
        print("latest_status write skipped:", str(e).strip().splitlines()[0] if str(e).strip() else e)

print("Fact rows:", fact_telemetry.count())
fact_telemetry.show(5, truncate=False)
print("Done: DWH v2 built from MinIO (history) and HBase (latest status)")