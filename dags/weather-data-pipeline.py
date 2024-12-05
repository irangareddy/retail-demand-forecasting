from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import json

from retail_data_sources.openweather.weather_data_processor import WeatherDataProcessor  # Assume this is the imported module for WeatherDataProcessor

# v1.0.9

def return_snowflake_conn():
    """Return Snowflake connection cursor using hook."""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()


def return_weather_processor():
    """Return WeatherDataProcessor instance using API Key."""
    api_key = Variable.get("OPEN_WEATHER_API_KEY")
    return WeatherDataProcessor(api_key)


@task
def fetch_weather(processor, state: str, year: str) -> dict:
    """Fetch weather data from OpenWeather API for a specific state and year."""
    try:
        data = processor.process_data()

        # If needed, you can filter by year or other conditions here
        # For example: filter_data = {year: data} (assuming you process it for specific years)

        logging.info(f"Successfully fetched weather data for {state} in {year}")
        
        # Return data as a dictionary (this is the key change)
        return {"weather_data": data}
    except Exception as e:
        logging.error(f"Error fetching weather data: {e}")
        raise


@task
def transform_weather(data: dict) -> list:
    """Transform weather data into Snowflake-compatible format."""
    try:
        records = []
        weather_data = data.get("weather_data", [])

        for state_data in weather_data:
            state_name = state_data["state_name"]
            for month, month_data in state_data["monthly_weather"].items():
                record = (
                    state_name,
                    month,
                    month_data["temp"]["mean"],  # Example: Using mean temperature
                    month_data["pressure"]["mean"],  # Example: Using mean pressure
                    month_data["humidity"]["mean"],  # Example: Using mean humidity
                    month_data["wind"]["mean"],  # Example: Using mean wind speed
                    month_data["precipitation"]["mean"],  # Example: Using mean precipitation
                    month_data["clouds"]["mean"],  # Example: Using mean cloud coverage
                    month_data["sunshine_hours_total"],  # Example: Total sunshine hours
                )
                records.append(record)

        logging.info(f"Successfully transformed {len(records)} records")
        return records
    except Exception as e:
        logging.error(f"Error transforming weather data: {e}")
        raise


@task
def load_weather(cursor, records: list, target_table: str) -> None:
    """Load transformed weather data into Snowflake."""
    try:
        cursor.execute("BEGIN")
        
        # Create table if not exists
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            state VARCHAR(2) NOT NULL,
            month VARCHAR(2) NOT NULL,
            mean_temp FLOAT NOT NULL,
            mean_pressure FLOAT NOT NULL,
            mean_humidity FLOAT NOT NULL,
            mean_wind FLOAT NOT NULL,
            mean_precipitation FLOAT NOT NULL,
            mean_clouds FLOAT NOT NULL,
            sunshine_hours_total FLOAT NOT NULL,
            loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (state, month)
        );
        """
        cursor.execute(create_table_sql)

        # Use merge for idempotency
        for record in records:
            merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING (
                SELECT
                    %s as state,
                    %s as month,
                    %s as mean_temp,
                    %s as mean_pressure,
                    %s as mean_humidity,
                    %s as mean_wind,
                    %s as mean_precipitation,
                    %s as mean_clouds,
                    %s as sunshine_hours_total
            ) AS source
            ON target.state = source.state AND target.month = source.month
            WHEN MATCHED THEN
                UPDATE SET
                    mean_temp = source.mean_temp,
                    mean_pressure = source.mean_pressure,
                    mean_humidity = source.mean_humidity,
                    mean_wind = source.mean_wind,
                    mean_precipitation = source.mean_precipitation,
                    mean_clouds = source.mean_clouds,
                    sunshine_hours_total = source.sunshine_hours_total,
                    loaded_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (state, month, mean_temp, mean_pressure, mean_humidity, 
                        mean_wind, mean_precipitation, mean_clouds, sunshine_hours_total)
                VALUES (source.state, source.month, source.mean_temp, source.mean_pressure, 
                        source.mean_humidity, source.mean_wind, source.mean_precipitation, 
                        source.mean_clouds, source.sunshine_hours_total)
            """
            cursor.execute(merge_sql, record)
            
        cursor.execute("COMMIT")
        logging.info(f"Successfully loaded {len(records)} records into Snowflake")
    except Exception as e:
        cursor.execute("ROLLBACK")
        logging.error(f"Error loading data into Snowflake: {e}")
        raise


with DAG(
    dag_id='weather_data_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': True,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description='A DAG to fetch weather data and load it into Snowflake',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 6 * * *',  # Run daily at 6 AM UTC
    catchup=False,
    tags=["weather"],
) as dag:

    target_table = "RETAIL_DATA_WAREHOUSE.RAW_DATA.WEATHER_STATS"
    current_year = str(datetime.now().year)
    state_code = "CA"  # Example state (can be dynamic based on your use case)

    # Initialize connections using hooks
    weather_processor = return_weather_processor()
    snowflake_conn = return_snowflake_conn()

    # Define tasks with connections
    fetch_task = fetch_weather(weather_processor, state_code, current_year)
    transform_task = transform_weather(fetch_task)
    load_task = load_weather(snowflake_conn, transform_task, target_table)

    # Set task dependencies
    fetch_task >> transform_task >> load_task
