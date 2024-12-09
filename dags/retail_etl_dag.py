from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='retail_etl_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': True,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description='A DAG to trigger multiple ETL pipelines (Census, Fred, OpenWeather)',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 8 5 * *',  # Runs at 8 AM on the 5th of every month
    catchup=False,
    tags=["retail", "etl"],
) as dag:

    # Dummy task to start the retail ETL pipeline
    start_task = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # Trigger the census retail sales task in census_retail_sales_pipeline DAG
    trigger_census_task = TriggerDagRunOperator(
        task_id='trigger_census_retail_sales_pipeline',
        trigger_dag_id='census_retail_sales_pipeline',  # DAG to trigger
        conf={'task': 'fetch_census'},  # Passing specific task name to the triggered DAG
        dag=dag,
    )

    # Trigger the Fred task in the 'fred' DAG
    trigger_fred_task = TriggerDagRunOperator(
        task_id='trigger_fred',
        trigger_dag_id='fred_economic_data_pipeline',  # DAG to trigger
        conf={'task': 'fred_task'},  # Passing specific task name to the triggered DAG
        dag=dag,
    )

    # Trigger the OpenWeather task in the 'openweather' DAG
    trigger_openweather_task = TriggerDagRunOperator(
        task_id='trigger_openweather',
        trigger_dag_id='weather_data_pipeline',  # DAG to trigger
        conf={'task': 'openweather_task'},  # Passing specific task name to the triggered DAG
        dag=dag,
        wait_for_completion=True
    )

    trigger_dbt_task = TriggerDagRunOperator(
        task_id='trigger_dbt_pipeline',
        trigger_dag_id='retail_dbt_pipeline',  # DAG to trigger
        conf={'task': 'dbt_task'},  # Passing specific task name to the triggered DAG
        dag=dag,
        wait_for_completion=True
    )

    # Dummy task to mark the end of the retail ETL pipeline
    end_task = DummyOperator(
        task_id='end',
        dag=dag,
    )

    # Define task dependencies to control the order of execution
    start_task >> [
        trigger_census_task, 
        trigger_fred_task, 
        trigger_openweather_task
    ] >> trigger_dbt_task >> end_task
