FROM apache/airflow:2.10.5-python3.11

# Java is needed later to run Spark inside the container (step 2)
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir pyspark==4.3.0 happybase boto3