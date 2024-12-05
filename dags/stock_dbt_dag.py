from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import logging

def check_dbt_results(context):
    task_instance = context['task_instance']
    result = task_instance.xcom_pull(task_ids='dbt_test')
    if 'Failed' in str(result):
        raise Exception('DBT tests failed!')

with DAG(
    'stock_dbt_pipeline',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='dbt transformations and snapshots for stock data',
    schedule_interval=None,  # Triggered by stock_data_pipeline
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    # Install dependencies
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/airflow/dbt && dbt deps'
    )

    # Debug dbt project
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command='cd /opt/airflow/dbt && dbt debug'
    )

    # Run models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt && dbt run'
    )

    # Test models
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt && dbt test',
        on_failure_callback=check_dbt_results
    )

    # Take snapshots after successful model run and tests
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command='cd /opt/airflow/dbt && dbt snapshot'
    )

    # Generate documentation
    dbt_docs = BashOperator(
        task_id='dbt_docs',
        bash_command='cd /opt/airflow/dbt && dbt docs generate'
    )

    # Define the corrected execution order
    dbt_deps >> dbt_debug >> dbt_run >> dbt_test >> dbt_snapshot >> dbt_docs