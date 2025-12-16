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
    'maintenance_silver_by_day',
    default_args=default_args,
    description='Daily maintenance for Silver table',
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['maintenance', 'silver', 'daily'],
)

silver_by_day = BashOperator(
    task_id='compact_silver_binpack',
    bash_command="""
    spark-submit \
        --master local[2] \
        --conf spark.driver.memory=1g \
        --conf spark.executor.memory=1g \
        --name maintenance_silver_by_day \
        --jars "/opt/airflow/dags/spark_jobs/utils/jars/*" \
        /opt/airflow/dags/spark_jobs/maintenance/maintenance_silver_by_day.py
    """,
    dag=dag,
)