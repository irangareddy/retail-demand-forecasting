"""Airflow DAG for fetching FRED economic data and loading to Snowflake."""

# version 1.0.17

from pathlib import Path
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
from typing import Any, Dict, Optional

from retail_data_sources.fred.fred_api_handler import FREDAPIHandler

# Constants
FRED_BASE_DIR = "/opt/airflow/data/fred"
TARGET_TABLE = "RETAIL_DATA_WAREHOUSE.RAW_DATA.FRED_ECONOMIC_DATA"

class CustomFREDAPIHandler(FREDAPIHandler):
    """Extended FRED API handler with configurable base directory."""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        rules_dict: Optional[dict] = None,
        base_dir: str = FRED_BASE_DIR
    ):
        super().__init__(api_key=api_key, rules_dict=rules_dict)
        self.fetcher.output_dir = base_dir

@task
def setup_directories() -> None:
    """Create necessary directories for FRED data processing."""
    try:
        base_dir = Path(FRED_BASE_DIR)
        base_dir.mkdir(parents=True, exist_ok=True)
        
        tmp_dir = base_dir / "tmp"
        tmp_dir.mkdir(exist_ok=True)
        
        logging.info(f"Created directory structure at {FRED_BASE_DIR}")
    except Exception as e:
        logging.error(f"Error in directory setup: {e}")
        raise

@task
def fetch_fred_data() -> Dict[str, Dict[str, Any]]:
    """Fetch FRED economic data."""
    try:
        api_key = Variable.get("FRED_API_KEY")
        handler = CustomFREDAPIHandler(api_key=api_key, base_dir=FRED_BASE_DIR)
        
        data = handler.process_data(fetch=True)
        if not data:
            raise ValueError("No FRED data fetched")
        logging.info("Successfully fetched FRED data")
        return data
    except Exception as e:
        logging.error(f"Error fetching FRED data: {e}")
        raise

@task
def transform_and_load(data: Dict[str, Dict[str, Any]], target_table: str) -> None:
    """Transform and load FRED data into Snowflake."""
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("BEGIN")

        # Create table if not exists
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            month VARCHAR(10) NOT NULL,
            consumer_confidence FLOAT,
            unemployment_rate FLOAT,
            inflation_rate FLOAT,
            gdp_growth_rate FLOAT,
            federal_funds_rate FLOAT,
            retail_sales FLOAT,
            last_updated TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (month)
        );
        """
        cursor.execute(create_table_sql)

        # Process each month's data
        for month, values in data.items():
            # Extract numeric values from the nested data structure
            processed_values = {
                'consumer_confidence': None,
                'unemployment_rate': None,
                'inflation_rate': None,
                'gdp_growth_rate': None,
                'federal_funds_rate': None,
                'retail_sales': None
            }
            
            for metric, metric_data in values.items():
                if isinstance(metric_data, dict) and 'value' in metric_data:
                    processed_values[metric] = metric_data['value']

            # Create record tuple with values in the correct order
            record = (
                month,
                processed_values['consumer_confidence'],
                processed_values['unemployment_rate'],
                processed_values['inflation_rate'],
                processed_values['gdp_growth_rate'],
                processed_values['federal_funds_rate'],
                processed_values['retail_sales']
            )

            merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING (
                SELECT
                    column1 as month,
                    column2 as consumer_confidence,
                    column3 as unemployment_rate,
                    column4 as inflation_rate,
                    column5 as gdp_growth_rate,
                    column6 as federal_funds_rate,
                    column7 as retail_sales
                FROM VALUES(%s, %s, %s, %s, %s, %s, %s)
            ) AS source
            ON target.month = source.month
            WHEN MATCHED THEN
                UPDATE SET
                    consumer_confidence = source.consumer_confidence,
                    unemployment_rate = source.unemployment_rate,
                    inflation_rate = source.inflation_rate,
                    gdp_growth_rate = source.gdp_growth_rate,
                    federal_funds_rate = source.federal_funds_rate,
                    retail_sales = source.retail_sales,
                    last_updated = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (
                    month, consumer_confidence, unemployment_rate, 
                    inflation_rate, gdp_growth_rate, federal_funds_rate, 
                    retail_sales
                )
                VALUES (
                    source.month, source.consumer_confidence, source.unemployment_rate,
                    source.inflation_rate, source.gdp_growth_rate, source.federal_funds_rate,
                    source.retail_sales
                )
            """
            cursor.execute(merge_sql, record)

        cursor.execute("COMMIT")
        logging.info("Successfully loaded FRED data into Snowflake")
    except Exception as e:
        if 'cursor' in locals():
            cursor.execute("ROLLBACK")
        logging.error(f"Error loading FRED data into Snowflake: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# DAG definition
with DAG(
    dag_id='fred_economic_data_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': True,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description='Pipeline to fetch FRED economic data and load to Snowflake',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 6 * * *',  # Run daily at 6 AM UTC
    catchup=False,
    tags=["fred", "economic_data"],
) as dag:
    
    # Create task instances and set dependencies
    setup = setup_directories()
    fred_data = fetch_fred_data()
    load_data = transform_and_load(fred_data, TARGET_TABLE)

    # Set task dependencies
    setup >> fred_data >> load_data