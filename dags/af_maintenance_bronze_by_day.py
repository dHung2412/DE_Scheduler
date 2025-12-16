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
    'maintenance_bronze_by_day',
    default_args=default_args,
    description='Daily maintenance for Bronze table',
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['maintenance', 'bronze', 'daily'],
)

bronze_by_day = BashOperator(
    task_id='compact_bronze_binpack',
    bash_command="""
    spark-submit \
        --master local[2] \
        --conf spark.driver.memory=1g \
        --conf spark.executor.memory=1g \
        --name maintenance_bronze_by_day \
        --jars "/opt/airflow/dags/spark_jobs/utils/jars/*" \
        /opt/airflow/dags/spark_jobs/maintenance/maintenance_bronze_by_day.py
    """,
    dag=dag,
)