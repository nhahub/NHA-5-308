import socket
from datetime import datetime

import boto3
import happybase
from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.client import Config

HOST = "host.docker.internal"   # the Windows machine, seen from inside Docker


def check_kafka():
    socket.create_connection((HOST, 9092), timeout=5).close()
    print("Kafka port 9092 is reachable")


def check_hbase():
    conn = happybase.Connection(HOST, port=9090, timeout=10000)
    print("HBase tables:", conn.tables())
    conn.close()


def check_minio():
    s3 = boto3.client(
        "s3", endpoint_url=f"http://{HOST}:9000",
        aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin",
        config=Config(signature_version="s3v4"), region_name="us-east-1",
    )
    s3.head_bucket(Bucket="fleet-bucket")
    print("MinIO bucket 'fleet-bucket' is reachable")


with DAG(
    dag_id="fleet_health_check",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # manual trigger for now
    catchup=False,
    tags=["fleet"],
) as dag:
    kafka = PythonOperator(task_id="check_kafka", python_callable=check_kafka)
    hbase = PythonOperator(task_id="check_hbase", python_callable=check_hbase)
    minio = PythonOperator(task_id="check_minio", python_callable=check_minio)