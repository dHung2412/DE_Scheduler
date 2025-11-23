FROM apache/airflow:2.10.3-python3.10

# Switch to root to install system packages
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install Airflow providers with constraints to avoid dependency conflicts
# This ensures all Airflow-related packages are compatible
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.10.txt" \
    apache-airflow-providers-postgres \
    apache-airflow-providers-apache-kafka \
    apache-airflow-providers-apache-spark

# Copy and install custom requirements
# These are installed AFTER Airflow providers to avoid conflicts
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt