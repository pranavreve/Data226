# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import snowflake.connector
import requests
from datetime import datetime, timedelta
import yfinance as yf
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_next_day(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    next_day = date_obj + timedelta(days=1)
    return next_day.strftime("%Y-%m-%d")

def get_previous_trading_day(date):
    one_day = timedelta(days=1)
    previous_day = date - one_day
    while previous_day.weekday() > 4:  # Monday is 0, Friday is 4
        previous_day -= one_day
    return previous_day

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    try:
        conn = hook.get_conn()
        logger.info("Successfully connected to Snowflake")
        return conn.cursor()
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}")
        raise

def get_logical_date():
    context = get_current_context()
    return str(context['logical_date'])[:10]

@task
def extract(symbol):
    end_date = datetime.now().date()
    start_date = get_previous_trading_day(end_date)
    logger.info(f"Fetching data for {symbol} from {start_date} to {end_date}")
    data = yf.download(symbol, start=start_date, end=end_date)
    if data.empty:
        logger.warning(f"No data available for {symbol} between {start_date} and {end_date}")
        return None
    return data.to_dict(orient="list")

@task
def load(d, symbol, target_table):
    if d is None:
        logger.warning("No data to load. Skipping.")
        return

    date = get_logical_date()
    cur = return_snowflake_conn()

    try:
        # Create table if it doesn't exist
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
            date date, open float, close float, high float, low float, volume bigint, symbol varchar 
        )""")
        
        # Start transaction
        cur.execute("BEGIN;")
        
        # Clear all existing data
        logger.info(f"Clearing all existing data from {target_table}")
        cur.execute(f"TRUNCATE TABLE {target_table}")
        
        # Insert new data
        sql = f"""INSERT INTO {target_table} (date, open, close, high, low, volume, symbol) VALUES (
          '{date}', {d['Open'][0]}, {d['Close'][0]}, {d['High'][0]}, {d['Low'][0]}, {d['Volume'][0]}, '{symbol}')"""
        logger.info(f"Executing SQL: {sql}")
        cur.execute(sql)
        
        # Commit transaction
        cur.execute("COMMIT;")
        logger.info(f"Data successfully loaded into Snowflake for {symbol} on {date}")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logger.error(f"Error in load function: {str(e)}")
        logger.error(f"Data being inserted: {d}")
        raise e

with DAG(
    dag_id = 'YfinanceToSnowflake',
    start_date = datetime(2024,10,2),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    target_table = "DEV.RAW_DATA.STOCK_PRICE"
    symbol = "AAPL"

    data = extract(symbol)
    load(data, symbol, target_table)
