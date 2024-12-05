from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable
from airflow.decorators import task
from airflow.utils.timezone import make_aware
from datetime import datetime, timedelta
import requests
import logging

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()

@task
def extract(symbols, api_key):
    all_results = {}
    try:
        for symbol in symbols:
            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
            res = requests.get(url).json()
            if "Time Series (Daily)" not in res:
                raise Exception(f"Invalid API response for {symbol}")
            
            results = []
            for d in res["Time Series (Daily)"]:
                daily_info = res["Time Series (Daily)"][d]
                daily_info['6. date'] = d
                daily_info['7. symbol'] = symbol
                results.append(daily_info)
            all_results[symbol] = results[-90:]
            logging.info(f"Successfully extracted data for {symbol}")
    except Exception as e:
        logging.error(f"Error extracting data for {symbol}: {e}")
        raise
    return all_results

@task
def transform(data):
    entries = []
    try:
        for symbol, entry_list in data.items():
            entries.extend(entry_list)
        logging.info(f"Successfully transformed {len(entries)} records")
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        raise
    return entries

@task
def load(con, results, target_table):
    try:
        con.execute("BEGIN")
        
        # Create table if not exists with basic schema
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            symbol VARCHAR(10) NOT NULL,
            date TIMESTAMP_NTZ NOT NULL,
            open DECIMAL(10, 4) NOT NULL,
            high DECIMAL(10, 4) NOT NULL,
            low DECIMAL(10, 4) NOT NULL,
            close DECIMAL(10, 4) NOT NULL,
            volume BIGINT NOT NULL,
            PRIMARY KEY (symbol, date)
        )""")

        # Use merge for idempotency
        for r in results:
            merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING (
                SELECT 
                    %(symbol)s as symbol,
                    %(date)s::TIMESTAMP_NTZ as date,
                    %(open)s::DECIMAL(10,4) as open,
                    %(high)s::DECIMAL(10,4) as high,
                    %(low)s::DECIMAL(10,4) as low,
                    %(close)s::DECIMAL(10,4) as close,
                    %(volume)s::BIGINT as volume
            ) AS source
            ON target.symbol = source.symbol AND target.date = source.date
            WHEN MATCHED THEN
                UPDATE SET
                    open = source.open,
                    high = source.high,
                    low = source.low,
                    close = source.close,
                    volume = source.volume
            WHEN NOT MATCHED THEN
                INSERT (symbol, date, open, high, low, close, volume)
                VALUES (
                    source.symbol, source.date, source.open, source.high,
                    source.low, source.close, source.volume
                )"""
            
            con.execute(merge_sql, {
                'symbol': r['7. symbol'],
                'date': r['6. date'],
                'open': float(r['1. open']),
                'high': float(r['2. high']),
                'low': float(r['3. low']),
                'close': float(r['4. close']),
                'volume': int(r['5. volume'])
            })
            
        con.execute("COMMIT")
        logging.info("Successfully loaded data into Snowflake")
    except Exception as e:
        con.execute("ROLLBACK")
        logging.error(f"Error loading data: {e}")
        raise

with DAG(
    dag_id='stock_data_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A DAG to fetch and process US stock market data after trading hours',
    start_date=make_aware(datetime(2024, 10, 10)),
    schedule_interval='30 21 * * 1-5',
    catchup=False
) as dag:

    target_table = "dev.raw_data.alphavantage_stockprice"
    api_key = Variable.get("alpha_vantage_api")
    symbols = Variable.get("stock_symbols", default_var=['NVDA', 'ORCL'])

    extract_task = extract(symbols, api_key)
    transform_task = transform(extract_task)
    load_task = load(return_snowflake_conn(), transform_task, target_table)

    # Define the execution order
    extract_task >> transform_task >> load_task

    # Trigger dbt pipeline after successful load
    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt_pipeline',
        trigger_dag_id='stock_dbt_pipeline',
        wait_for_completion=True
    )

    load_task >> trigger_dbt