from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
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

bronze_by_week = BashOperator(
    task_id='expire_and_cleanup',
    bash_command="""
    spark-submit \
        --master local[2] \
        --conf spark.driver.memory=4g \
        --conf spark.executor.memory=4g \
        --name maintenance_bronze_by_week \
        --jars "/opt/airflow/dags/spark_jobs/utils/jars/*" \
        /opt/airflow/dags/spark_jobs/maintenance/maintenance_bronze_by_week.py
    """,
    dag=dag,
)

