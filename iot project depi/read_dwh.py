import os
import sys

# Tell Spark which Python to use (Windows) + Hadoop/winutils paths
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession

DWH = "s3a://fleet-bucket/dwh/"
TABLES = ["dim_vehicle", "dim_driver", "dim_trip", "dim_time", "fact_telemetry", "latest_status"]

# Example queries (run automatically when you start the script)
QUERIES = {
    "Rows per table": """
        SELECT 'fact_telemetry' AS table_name, COUNT(*) AS rows FROM fact_telemetry
        UNION ALL SELECT 'dim_vehicle', COUNT(*) FROM dim_vehicle
        UNION ALL SELECT 'dim_driver', COUNT(*) FROM dim_driver
        UNION ALL SELECT 'dim_trip', COUNT(*) FROM dim_trip
        UNION ALL SELECT 'dim_time', COUNT(*) FROM dim_time
    """,
    "Delay status (distinct bookings)": """
        SELECT delay_status, COUNT(DISTINCT BookingID) AS bookings, COUNT(*) AS pings
        FROM fact_telemetry
        GROUP BY delay_status
        ORDER BY bookings DESC
    """,
    "Top 10 vehicles by pings": """
        SELECT v.vehicle_no, COUNT(*) AS pings
        FROM fact_telemetry f
        JOIN dim_vehicle v ON f.vehicle_key = v.vehicle_key
        GROUP BY v.vehicle_no
        ORDER BY pings DESC
        LIMIT 10
    """,
    "Top 10 drivers by delayed bookings": """
        SELECT d.driver_name, COUNT(DISTINCT f.BookingID) AS delayed_bookings
        FROM fact_telemetry f
        JOIN dim_driver d ON f.driver_key = d.driver_key
        WHERE f.delay_status = 'Delayed'
        GROUP BY d.driver_name
        ORDER BY delayed_bookings DESC
        LIMIT 10
    """,
    "Top 10 materials shipped": """
        SELECT material, COUNT(*) AS bookings, ROUND(AVG(distance_km), 1) AS avg_km
        FROM dim_trip
        GROUP BY material
        ORDER BY bookings DESC
        LIMIT 10
    """,
    "Busiest routes": """
        SELECT origin, destination, COUNT(*) AS bookings, ROUND(AVG(distance_km), 1) AS avg_km
        FROM dim_trip
        GROUP BY origin, destination
        ORDER BY bookings DESC
        LIMIT 10
    """,
    "Pings per month": """
        SELECT t.year, t.month, COUNT(*) AS pings
        FROM fact_telemetry f
        JOIN dim_time t ON f.time_key = t.time_key
        GROUP BY t.year, t.month
        ORDER BY t.year, t.month
    """,
    "Last known position per vehicle (first 10)": """
        SELECT vehicle_no, event_time, lat, lon
        FROM (
            SELECT v.vehicle_no, f.event_time, f.lat, f.lon,
                   ROW_NUMBER() OVER (PARTITION BY f.vehicle_key
                                      ORDER BY f.event_time DESC) AS rn
            FROM fact_telemetry f
            JOIN dim_vehicle v ON f.vehicle_key = v.vehicle_key
        ) x
        WHERE rn = 1
        LIMIT 10
    """,
}

def short_err(e):
    """First non-empty lines of the error (Spark messages often start with a blank line)."""
    lines = [l for l in str(e).splitlines() if l.strip()]
    return "\n".join(lines[:8])

os.makedirs("C:\\spark-temp", exist_ok=True)

spark = (
    SparkSession.builder
    .appName("Fleet_DWH_Query")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.local.dir", "C:\\spark-temp")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Register every Parquet table as a SQL view
loaded = []
for name in TABLES:
    try:
        spark.read.parquet(DWH + name).createOrReplaceTempView(name)
        loaded.append(name)
    except Exception as e:
        print(f"[skip] {name}: {short_err(e)}")
print("Tables available:", loaded)

# 1) Run the example queries
for title, sql in QUERIES.items():
    print(f"\n=== {title} ===")
    try:
        spark.sql(sql).show(20, truncate=False)
    except Exception as e:
        print("Query failed:", short_err(e))

# 2) Interactive mode: type your own SQL (one line), 'exit' to quit
print("\nInteractive mode. Type a SQL query on ONE line, or 'tables', or 'exit'.")
print("Example: SELECT vehicle_no, fleet_id FROM dim_vehicle LIMIT 5")
print("Prefix with v for vertical output, or csv to save to query_result.csv")
while True:
    try:
        sql = input("\nspark-sql> ").strip()
    except (EOFError, KeyboardInterrupt):
        break
    if not sql:
        continue
    if sql.lower() in ("exit", "quit"):
        break
    if sql.lower() == "tables":
        print(loaded)
        continue
    # prefixes:  "v <sql>"   = vertical output (one column per line, best for wide tables)
    #            "csv <sql>" = save the result to query_result.csv (open it in Excel)
    mode = "table"
    if sql.lower().startswith("v "):
        mode, sql = "vertical", sql[2:].strip()
    elif sql.lower().startswith("csv "):
        mode, sql = "csv", sql[4:].strip()
    try:
        result = spark.sql(sql.rstrip(";"))
        if mode == "vertical":
            result.show(5, truncate=False, vertical=True)
        elif mode == "csv":
            result.toPandas().to_csv("query_result.csv", index=False, encoding="utf-8-sig")
            print("Saved to query_result.csv in the current folder")
        else:
            result.show(50, truncate=False)
    except Exception as e:
        print("Query failed:", short_err(e))

spark.stop()
print("Bye.")
