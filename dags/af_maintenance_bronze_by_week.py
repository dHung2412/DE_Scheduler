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
    'maintenance_bronze_by_week',
    default_args=default_args,
    description='Weekly maintenance for Bronze table',
    schedule='@weekly',
    catchup=False,
    max_active_runs=1,
    tags=['maintenance', 'bronze', 'weekly'],
)

bronze_by_week = SparkSubmitOperator(
    task_id='expire_and_cleanup',
    application='/opt/airflow/dags/spark_jobs/maintenance/maintenance_bronze_by_week.py',
    name='maintenance_bronze_by_week',
    conn_id='spark_default',
    verbose=True,
    conf={
        'spark.master': 'local[2]',
        'spark.driver.memory': '4g',
        'spark.executor.memory': '4g',
    },
    dag=dag,
)

