# Sử dụng Airflow 3.1.3 (từ file compose mới) với Python 3.11 (để ổn định)
FROM apache/airflow:3.1.3-python3.11

# Switch to root to install packages
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get clean

# Switch back to the airflow user
USER airflow

# -----------------------------------------------------------------
# BƯỚC 1: Cài đặt các thư viện lõi gây xung đột TRƯỚC
# Chúng ta "pin" các phiên bản này để hướng dẫn pip
# -----------------------------------------------------------------
RUN pip install --no-cache-dir \
    "pyspark~=3.4.0" \
    "pyarrow>=14.0.1" \
    "pyiceberg~=0.5.0"

# -----------------------------------------------------------------
# BƯỚC 2: Cài đặt các gói còn lại
# pip sẽ sử dụng các gói đã cài ở Bước 1
# -----------------------------------------------------------------
RUN pip install --no-cache-dir \
    # Airflow providers
    apache-airflow-providers-postgres \
    apache-airflow-providers-apache-kafka \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-sftp \
    # DBT and adapters
    dbt-postgres \
    "dbt-spark[iceberg]~=1.8.0" \
    # Kafka client
    kafka-python \
    # S3
    boto3 \
    fastavro