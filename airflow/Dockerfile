FROM apache/airflow:2.10.3-python3.10

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    git \
    gcc \
    g++ \
    libsasl2-dev \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.10.txt" \
    apache-airflow-providers-postgres \
    apache-airflow-providers-apache-kafka \
    apache-airflow-providers-apache-spark

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt