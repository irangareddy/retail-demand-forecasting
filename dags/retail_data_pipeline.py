# """DAG for optimized JSON loading into Snowflake."""

# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.decorators import task
# from airflow.models import Variable
# from datetime import datetime, timedelta
# import json
# import logging

# from retail_data_sources.census.retail_sales_processor import RetailSalesProcessor
# from retail_data_sources.census.models.retail_sales import RetailReport
# from retail_data_sources.fred.fred_api_handler import FREDAPIHandler

# @task
# def fetch_census_data():
#     """Fetch retail sales data from Census API."""
#     logging.info("Starting to fetch Census data...")
#     try:
#         api_key = Variable.get("CENSUS_API_KEY")
#         census_processor = RetailSalesProcessor(api_key=api_key)
#         current_year = datetime.now().year
#         data: RetailReport = census_processor.process_data([str(current_year)])
#         logging.info("Successfully fetched Census data")
#         return data.to_dict()
#     except Exception as e:
#         logging.error(f"Error fetching Census data: {e}")
#         raise

# @task
# def save_to_snowflake(data: dict):
#     """Save Census data efficiently to Snowflake using best practices."""
#     try:
#         hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
#         # Create table with optimized structure
#         create_table_sql = """
#         CREATE TABLE IF NOT EXISTS RAW_CENSUS_DATA (
#             id NUMBER AUTOINCREMENT,
#             census_data VARIANT NOT NULL,
#             ingest_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
#             batch_id STRING DEFAULT UUID_STRING(),
#             CONSTRAINT raw_census_pk PRIMARY KEY (id)
#         );
#         """
        
#         # Simplified insert using just PARSE_JSON
#         insert_sql = """
#         INSERT INTO RAW_CENSUS_DATA (census_data)
#         SELECT PARSE_JSON(%s);
#         """
        
#         # Execute statements
#         conn = hook.get_conn()
#         cur = conn.cursor()
        
#         try:
#             # Execute DDL statements
#             cur.execute(create_table_sql)
            
#             # Insert data
#             json_data = json.dumps(data)
#             cur.execute(insert_sql, (json_data,))
            
#             conn.commit()
#             logging.info("Successfully saved Census data to Snowflake")
            
#         finally:
#             cur.close()
#             conn.close()
            
#     except Exception as e:
#         logging.error(f"Error saving to Snowflake: {e}")
#         raise


# @task
# def fetch_fred_data():
#     """Fetch economic metrics from FRED API."""
#     logging.info("Starting to fetch FRED economic data...")
#     try:
#         api_key = Variable.get("FRED_API_KEY")
#         handler = FREDAPIHandler(api_key=api_key)
#         economic_data = handler.process_data(fetch=True)
#         logging.info("Successfully fetched FRED data")
#         return economic_data.to_dict()
#     except Exception as e:
#         logging.error(f"Error fetching FRED data: {e}")
#         raise

# @task
# def save_raw_to_snowflake(data: dict):
#     """Save raw FRED data to Snowflake."""
#     try:
#         hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
#         conn = hook.get_conn()
#         cur = conn.cursor()
        
#         # Create raw table if not exists
#         create_raw_table_sql = """
#         CREATE TABLE IF NOT EXISTS RAW.FRED_DATA (
#             id NUMBER AUTOINCREMENT,
#             economic_data VARIANT NOT NULL,
#             fetch_date DATE DEFAULT CURRENT_DATE(),
#             inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
#             batch_id STRING DEFAULT UUID_STRING(),
#             CONSTRAINT raw_fred_pk PRIMARY KEY (id)
#         );
#         """
        
#         cur.execute(create_raw_table_sql)
        
#         # Insert raw data
#         insert_raw_sql = """
#         INSERT INTO RAW.FRED_DATA (economic_data) 
#         SELECT PARSE_JSON(%s);
#         """
#         cur.execute(insert_raw_sql, (json.dumps(data),))
        
#         conn.commit()
#         logging.info("Successfully saved raw FRED data")
        
#     except Exception as e:
#         logging.error(f"Error saving raw data: {e}")
#         raise
#     finally:
#         cur.close()
#         conn.close()

# with DAG(
#     'census_to_snowflake_optimized',
#     default_args={
#         'owner': 'airflow',
#         'retries': 1,
#         'retry_delay': timedelta(minutes=5),
#     },
#     description='Fetch Census data and save as JSON in Snowflake with optimizations',
#     schedule_interval='0 1 * * *',  # Run daily at 1 AM
#     start_date=datetime(2024, 1, 1),
#     catchup=False
# ) as dag:

#     # Define task dependencies
#     census_data = fetch_census_data()
#     fred_data = fetch_fred_data()

#     save_data = save_to_snowflake(census_data)
#     raw_save = save_raw_to_snowflake(fred_data)
    
#     # Set up complete pipeline
#     census_data >> save_data
#     fred_data >> raw_save

"""DAG for FRED data ingestion with proper rules file handling."""

# version: 1.0.1

from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
import shutil

from retail_data_sources.fred.fred_api_handler import FREDAPIHandler
from retail_data_sources.fred.models.economic_metrics import EconomicData
from retail_data_sources.snowflake.fred import FredSnowflake

# Constants
FRED_BASE_DIR = "/opt/airflow/data/fred"

@task
def setup_directories():
    """Create necessary directories and copy rules file."""
    try:
        # Create main data directory
        data_dir = Path(FRED_BASE_DIR)
        data_dir.mkdir(parents=True, exist_ok=True)
        
        # Create tmp subdirectory for FRED data
        tmp_dir = data_dir / "tmp"
        tmp_dir.mkdir(exist_ok=True)
        
        logging.info(f"Created directory structure at {FRED_BASE_DIR}")
    except Exception as e:
        logging.error(f"Error in setup: {e}")
        raise

@task
def fetch_fred_data():
    """Fetch economic metrics from FRED API."""
    logging.info("Starting to fetch FRED economic data...")
    try:
        api_key = Variable.get("FRED_API_KEY")
        rules_file = Path(FRED_BASE_DIR) / "fred_interpretation_rules.json"
        
        handler = FREDAPIHandler(
            api_key=api_key,
            base_dir=FRED_BASE_DIR,
            rules_file=str(rules_file)
        )
        economic_data = handler.process_data(fetch=True)
        if not economic_data:
            raise ValueError("No data returned from FRED API")
            
        logging.info("Successfully fetched FRED data")
        return economic_data.to_dict()
    except Exception as e:
        logging.error(f"Error fetching FRED data: {e}")
        raise

@task
def save_raw_to_snowflake(data: dict):
    """Save raw FRED data to Snowflake using FredSnowflake class."""
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        
        fred_snowflake = FredSnowflake()
        target_schema = Variable.get("SNOWFLAKE_TARGET_SCHEMA", "RAW")
        
        # Prepare SQL statements
        create_sql, records, merge_sql = fred_snowflake.prepare_load_sql(
            economic_data=data,
            target_schema=target_schema,
            target_table='FRED_ECONOMIC_DATA'
        )
        
        cur = conn.cursor()
        try:
            # Execute create statement
            cur.execute(create_sql)
            
            # Execute merge for each record
            for record in records:
                cur.execute(merge_sql, record)
            
            conn.commit()
            logging.info("Successfully saved raw FRED data to Snowflake")
            
        finally:
            cur.close()
            conn.close()
            
    except Exception as e:
        logging.error(f"Error saving raw data to Snowflake: {e}")
        raise

@task
def cleanup_directories():
    """Clean up temporary FRED data files."""
    try:
        tmp_dir = Path(FRED_BASE_DIR) / "tmp"
        if tmp_dir.exists():
            # import shutil
            # shutil.rmtree(tmp_dir)
            logging.info(f"Cleaned up temporary directory: {tmp_dir}")
    except Exception as e:
        logging.error(f"Error cleaning up directories: {e}")
        # Don't raise as this is cleanup

with DAG(
    'fred_to_snowflake_dbt',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='FRED data pipeline with dbt transformations',
    schedule_interval='0 1 * * *',  # Run daily at 1 AM
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    # Define task dependencies
    setup = setup_directories()
    fred_data = fetch_fred_data()
    raw_save = save_raw_to_snowflake(fred_data)
    cleanup = cleanup_directories()
    
    # Set up complete pipeline
    setup >> fred_data >> raw_save >> cleanup