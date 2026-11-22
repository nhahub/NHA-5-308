import os
import re
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

import happybase
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = "s3a://fleet-bucket/"
DWH = BASE + "dwh/"
SOURCE = BASE + "telemetry_data/*/*.json"

PII = {"Driver_MobileNo"}
IN_DIMS = {
    "vehicle_no", "customerNameCode", "Driver_Name",
    "Origin_Location", "Destination_Location",
    "Material_Shipped", "Data_Ping_time",
}


def clean_name(name):
    return re.sub(r"[^0-9a-zA-Z_]+", "_", str(name)).strip("_")


os.makedirs("C:\\spark-temp", exist_ok=True)

spark = (
    SparkSession.builder
    .appName("Fleet_DWH_Pipeline_v2")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.local.dir", "C:\\spark-temp")
    .config("spark.sql.sources.parallelPartitionDiscovery.threshold", "100000")
    .config("spark.sql.files.ignoreMissingFiles", "true")
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print("Spark session connected to MinIO")

print("Reading JSON from MinIO...")
raw = spark.read.format("json").load(SOURCE)
raw = raw.filter(F.col("record").isNotNull())
rec_cols = raw.select("record.*").columns
print("Excel columns found:", rec_cols)


def rec(name):
    if name in rec_cols:
        return F.col(f"record.`{name}`").cast("string")
    return F.lit(None).cast("string")


def num(name):
    if name in rec_cols:
        return F.expr(f"try_cast(`record`.`{name}` AS double)")
    return F.lit(None).cast("double")


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
    F.initcap(F.trim(rec("Material_Shipped"))).alias("material"),
    num("Curr_lat").alias("lat"),
    num("Curr_lon").alias("lon"),
    num("TRANSPORTATION_DISTANCE_IN_KM").alias("distance_km"),
    rec("delay").alias("delay"),
    rec("ontime").alias("ontime"),
    *[rec(c).alias(clean_name(c)) for c in other_cols],
).dropna(subset=["vehicle_no", "event_time"])

df = df.fillna({"origin": "Unknown", "destination": "Unknown",
                "driver_name": "Unknown", "material": "Unknown"})

df = df.withColumn(
    "delay_status",
    F.when(F.col("delay") == "R", "Delayed")
     .when(F.col("ontime") == "G", "On time")
     .otherwise("Unknown"),
)

df = df.dropDuplicates(["vehicle_no", "BookingID", "event_time"])
df = df.withColumn("time_hour", F.date_trunc("hour", "event_time"))
df = df.cache()
print("Rows loaded from MinIO:", df.count())


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

try:
    conn = happybase.Connection("localhost", port=9090)
    hbase_rows = sum(1 for _, data in conn.table("fleet_telemetry").scan() if b"d:BookingID" in data)
    conn.close()
    print("HBase active telemetry rows:", hbase_rows)
except Exception as e:
    print("HBase check skipped:", str(e).strip().splitlines()[0] if str(e).strip() else e)

latest_status = (
    df.groupBy("vehicle_no")
    .agg(F.max_by(F.struct(*[c for c in df.columns if c != "vehicle_no"]), "event_time").alias("latest"))
    .select("vehicle_no", "latest.*")
)

print("Writing DWH tables to MinIO (Parquet)...")
dim_vehicle.write.mode("overwrite").parquet(DWH + "dim_vehicle")
dim_driver.write.mode("overwrite").parquet(DWH + "dim_driver")
dim_trip.write.mode("overwrite").parquet(DWH + "dim_trip")
dim_time.write.mode("overwrite").parquet(DWH + "dim_time")
fact_telemetry.write.mode("overwrite").parquet(DWH + "fact_telemetry")
latest_status.write.mode("overwrite").parquet(DWH + "latest_status")

print("Fact rows:", fact_telemetry.count())
fact_telemetry.show(5, truncate=False)
print("Done: DWH v2 built successfully!")
