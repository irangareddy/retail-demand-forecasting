from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

from retail_data_sources.census.retail_sales_processor import RetailSalesProcessor

# v1.0.10

def return_snowflake_conn():
    """Return Snowflake connection cursor using hook."""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()


def return_census_conn():
    """Return Census API connection using RetailSalesProcessor."""
    api_key = Variable.get("CENSUS_API_KEY")
    return RetailSalesProcessor(api_key)


@task
def fetch_census(processor, year: str) -> dict:
    """Fetch retail sales data from Census API."""
    try:
        data = processor.process_data([year])
        is_valid, validation_results = processor.validate_data(data)
        
        if not is_valid:
            raise ValueError(f"Data validation failed: {validation_results}")
        
        logging.info(f"Successfully fetched Census data for year {year}")
        return data
    except Exception as e:
        logging.error(f"Error fetching Census data: {e}")
        raise


@task
def transform_census(data: dict) -> list:
    """Transform Census data into Snowflake-compatible format."""
    try:
        records = []
        for month, month_data in data["sales_data"].items():
            national_445 = month_data["national_total"]["category_445"]
            national_448 = month_data["national_total"]["category_448"]
            last_updated = datetime.strptime(
                data["metadata"]["last_updated"], 
                "%Y-%m-%d"
            )

            for state, state_data in month_data["states"].items():
                record = (
                    month,
                    state,
                    state_data["category_445"]["sales_value"],
                    state_data["category_445"]["state_share"],
                    state_data["category_448"]["sales_value"],
                    state_data["category_448"]["state_share"],
                    national_445,
                    national_448,
                    last_updated
                )
                records.append(record)
        
        logging.info(f"Successfully transformed {len(records)} records")
        return records
    except Exception as e:
        logging.error(f"Error transforming Census data: {e}")
        raise


@task
def load_census(cursor, records: list, target_table: str) -> None:
    """Load transformed data into Snowflake."""
    try:
        cursor.execute("BEGIN")
        
        # Create table if not exists
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            month VARCHAR(10) NOT NULL,
            state VARCHAR(2) NOT NULL,
            category_445_sales FLOAT NOT NULL,
            category_445_share FLOAT NOT NULL,
            category_448_sales FLOAT NOT NULL,
            category_448_share FLOAT NOT NULL,
            national_445_total FLOAT NOT NULL,
            national_448_total FLOAT NOT NULL,
            last_updated TIMESTAMP_NTZ NOT NULL,
            loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (month, state)
        );
        """
        cursor.execute(create_table_sql)

        # Use merge for idempotency
        for record in records:
            merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING (
                SELECT
                    %s as month,
                    %s as state,
                    %s as category_445_sales,
                    %s as category_445_share,
                    %s as category_448_sales,
                    %s as category_448_share,
                    %s as national_445_total,
                    %s as national_448_total,
                    %s as last_updated
            ) AS source
            ON target.month = source.month AND target.state = source.state
            WHEN MATCHED THEN
                UPDATE SET
                    category_445_sales = source.category_445_sales,
                    category_445_share = source.category_445_share,
                    category_448_sales = source.category_448_sales,
                    category_448_share = source.category_448_share,
                    national_445_total = source.national_445_total,
                    national_448_total = source.national_448_total,
                    last_updated = source.last_updated,
                    loaded_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (month, state, category_445_sales, category_445_share, 
                       category_448_sales, category_448_share, national_445_total, 
                       national_448_total, last_updated)
                VALUES (source.month, source.state, source.category_445_sales,
                       source.category_445_share, source.category_448_sales,
                       source.category_448_share, source.national_445_total,
                       source.national_448_total, source.last_updated)
            """
            cursor.execute(merge_sql, record)
            
        cursor.execute("COMMIT")
        logging.info(f"Successfully loaded {len(records)} records into Snowflake")
    except Exception as e:
        cursor.execute("ROLLBACK")
        logging.error(f"Error loading data into Snowflake: {e}")
        raise


with DAG(
    dag_id='census_retail_sales_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': True,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description='A DAG to fetch Census retail sales data and load to Snowflake',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 6 * * *',  # Run daily at 6 AM UTC
    catchup=False,
    tags=["census", "retail_sales"],
) as dag:

    target_table = "RETAIL_DATA_WAREHOUSE.RAW_DATA.RETAIL_SALES"
    # from 2019 to the current year
    start_year = 2019
    current_year = datetime.now().year
    years_to_fetch = [str(year) for year in range(start_year, current_year + 1)]


    # Initialize connections using hooks
    census_conn = return_census_conn()
    snowflake_conn = return_snowflake_conn()

    # Define tasks with connections
    fetch_task = fetch_census(census_conn, years_to_fetch)
    transform_task = transform_census(fetch_task)
    load_task = load_census(snowflake_conn, transform_task, target_table)

    # Set task dependencies
    fetch_task >> transform_task >> load_task