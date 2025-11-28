"""
Daily Bronze Table Maintenance
- Binpack compaction for last 7 days
- Target file size: 20MB
- Minimum 5 input files
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'maintenance_bronze_by_day',
    default_args=default_args,
    description='Daily maintenance for Bronze table',
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['maintenance', 'bronze', 'daily'],
)

bronze_by_day = SparkSubmitOperator(
    task_id='compact_bronze_binpack',
    application='/opt/airflow/dags/spark_jobs/maintenance/maintenance_bronze_by_day.py',
    name='maintenance_bronze_by_day',
    conn_id='spark_default',
    verbose=True,
    conf={
        'spark.master': 'local[2]',
        'spark.driver.memory': '2g',
        'spark.executor.memory': '2g',
    },
    dag=dag,
)

