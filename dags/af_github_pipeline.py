"""
GitHub Events Pipeline DAG
Orchestrates the full Bronze -> Silver -> Gold pipeline

Schedule: Hourly
Tasks:
1. Spark: Bronze -> Silver transformation
2. dbt: Silver model refresh
3. dbt: Gold model refresh  
4. dbt: Data quality tests
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'github_events_pipeline',
    default_args=default_args,
    description='Hourly GitHub events processing pipeline',
    schedule='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['github', 'etl', 'hourly'],
)

# Task 1: Bronze to Silver (Spark)
bronze_to_silver = SparkSubmitOperator(
    task_id='bronze_to_silver_transformation',
    application='/opt/airflow/dags/spark_jobs/bronze_silver/process_bronze_to_silver.py',
    name='bronze_to_silver_job',
    conn_id='spark_default',
    verbose=True,
    conf={
        'spark.master': 'local[2]',
        'spark.driver.memory': '2g',
        'spark.executor.memory': '2g',
    },
    application_args=[],
    dag=dag,
)

# Task 2: dbt run - Silver models
dbt_run_silver = BashOperator(
    task_id='dbt_run_silver_models',
    bash_command='cd /opt/dbt_project && dbt run --models silver+',
    dag=dag,
)

# Task 3: dbt run - Gold models
dbt_run_gold = BashOperator(
    task_id='dbt_run_gold_models',
    bash_command='cd /opt/dbt_project && dbt run --models gold+',
    dag=dag,
)

# Task 4: dbt test - Data quality checks
dbt_test = BashOperator(
    task_id='dbt_test_data_quality',
    bash_command='cd /opt/dbt_project && dbt test',
    dag=dag,
)

# Task dependencies
bronze_to_silver >> dbt_run_silver >> dbt_run_gold >> dbt_test
