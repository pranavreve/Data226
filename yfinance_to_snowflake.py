from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import yfinance as yf

# Helper function to get the next day's date
def get_next_day(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    next_day = date_obj + timedelta(days=1)
    return next_day.strftime("%Y-%m-%d")

# Function to return Snowflake connection
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

# Function to get the current logical date
def get_logical_date():
    context = get_current_context()
    return str(context['logical_date'])[:10]

# Task to extract stock data from Yahoo Finance
@task
def extract(symbol):
    date = get_logical_date()
    end_date = get_next_day(date)
    
    # Download the stock data for the given date range (one day)
    data = yf.download(symbol, start=date, end=end_date)
    
    # Convert data to a dictionary for easy transfer between tasks
    return data.to_dict(orient="list")

# Task to load the stock data into Snowflake
@task
def load(d, symbol, target_table):
    date = get_logical_date()
    cur = return_snowflake_conn()

    try:
        # Create table if it does not exist
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
            date date, open float, close float, high float, low float, volume int, symbol varchar 
        )""")
        
        cur.execute("BEGIN;")
        
        # Delete existing data for that date if any
        cur.execute(f"DELETE FROM {target_table} WHERE date='{date}'")
        
        # Insert the new stock data
        sql = f"""INSERT INTO {target_table} (date, open, close, high, low, volume, symbol) VALUES (
          '{date}', {d['Open'][0]}, {d['Close'][0]}, {d['High'][0]}, {d['Low'][0]}, {d['Volume'][0]}, '{symbol}')"""
        
        cur.execute(sql)
        cur.execute("COMMIT;")
        
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Error occurred: {e}")
        raise e

# Define the DAG
with DAG(
    dag_id='YfinanceToSnowflake',
    start_date=datetime(2024, 10, 2),
    catchup=False,
    schedule_interval='30 2 * * *',  # Scheduled to run daily at 2:30 AM UTC
    tags=['ETL']
) as dag:
    target_table = "dev.raw_data.stock_price"  # Make sure this table matches your environment
    symbol = "AAPL"  # Modify this as needed

    # Define the extract and load tasks
    data = extract(symbol)
    load(data, symbol, target_table)
