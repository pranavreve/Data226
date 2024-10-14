# -*- coding: utf-8 -*-
"""
DAG for Stock Price Prediction Analytics using Snowflake & Airflow

This script creates an Airflow DAG that extracts stock price data from Alpha Vantage API, 
transforms it, and loads it into a Snowflake data warehouse. It also includes a machine 
learning model to forecast stock prices based on the historical data.

Author: Pranav Reveendran
"""

# Import necessary modules
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging
import snowflake.connector
import requests
from datetime import datetime, timedelta

# Function to establish a connection to Snowflake using the SnowflakeHook
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')  # Snowflake connection ID in Airflow
    conn = hook.get_conn()
    return conn.cursor()  # Cursor object to interact with the Snowflake database

# Task to initialize the target table in Snowflake (create if not exists)
@task
def initialize_target_table(table):
    cursor = return_snowflake_conn()  # Get Snowflake connection
    # SQL query to create or replace the table structure for stock data
    table_create = f"""
    CREATE OR REPLACE TABLE {table} (
        date TIMESTAMP_NTZ,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume INT,
        symbol STRING
    );
    """
    cursor.execute(table_create)  # Execute query to create the table
    print(f"Target Table '{table}' Initialized and Ready to Store Data from ETL")

# Task to extract stock data from Alpha Vantage API based on the provided stock symbol
@task
def extract_stock_data(symbol):
    print(f"Starting Extraction for {symbol} Stock Data...")  # Logging extraction initiation
    vantage_api_key = Variable.get('vantage_api_key')  # API key stored in Airflow Variables
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}"
    r = requests.get(url)  # Fetch stock data from Alpha Vantage API
    data = r.json()  # Parse JSON response
    print(f"<---------- Last Refreshed: {data['Meta Data']['3. Last Refreshed']} for {symbol} ---------->")
    results = []  # List to store stock data records
    for d in data["Time Series (Daily)"]:  # Loop through daily stock prices
        stock_info = data["Time Series (Daily)"][d]  # Get stock price information for each date
        stock_info["date"] = d  # Add the date field to the stock data
        results.append(stock_info)  # Append the stock info to the results list
    print(f"Stock Data Extraction Complete for {symbol}")
    return results

# Task to filter the extracted stock data to only include the last 90 days
@task
def transform_to_90d_stock_data(results):
    print("Transforming the Extracted Stock Data to Last 90 Days...")
    today = datetime.now().date()  # Get today's date
    ninety_days_ago = today - timedelta(days=90)  # Calculate the date 90 days ago
    # Filter results to keep only data from the last 90 days
    filtered_results = [
        entry for entry in results
        if datetime.strptime(entry["date"], "%Y-%m-%d").date() >= ninety_days_ago
    ]
    print("Completed Transformation of Stock Data to 90 Days")
    return filtered_results

# Task to load the transformed stock data into the Snowflake table
@task
def load_stock_data(table, results, symbol):
    try:
        cursor = return_snowflake_conn()  # Get Snowflake connection
        cursor.execute("BEGIN;")  # Start a transaction
        print(f"{symbol} Stock Data Ready to Load into Snowflake...")
        index = 1  # Initialize counter for records inserted
        # Loop through each record in the results and insert into Snowflake
        for r in results:
            open = r["1. open"]
            high = r["2. high"]
            low = r["3. low"]
            close = r["4. close"]
            volume = r["5. volume"]
            date = r["date"]
            # Use MERGE to either insert or update existing records
            merge_sql = f"""
            MERGE INTO {table} AS target
            USING (SELECT '{date}' AS date, '{open}' AS open, '{high}' AS high, '{low}' AS low, 
                   '{close}' AS close, '{volume}' AS volume, '{symbol}' AS symbol) AS source
            ON target.symbol = source.symbol AND target.date = source.date
            WHEN MATCHED THEN
                UPDATE SET
                    date = source.date,
                    open = source.open,
                    high = source.high,
                    low = source.low,
                    close = source.close,
                    volume = source.volume
            WHEN NOT MATCHED THEN
                INSERT (date, open, high, low, close, volume, symbol)
                VALUES (source.date, source.open, source.high, source.low, source.close, source.volume, source.symbol);
            """
            cursor.execute(merge_sql)  # Execute SQL for each record
            print(f"Inserted Record {index} for {symbol}")
            index += 1
        cursor.execute("COMMIT;")  # Commit the transaction
        print(f"Successfully Loaded {len(results)} Records into {table}")
    except Exception as e:
        cursor.execute("ROLLBACK;")  # Rollback in case of an error
        print(f"Error Occurred During Load: {e}")

# Task to train the ML model based on the stock data in Snowflake
@task
def train(cursor, train_input_table, train_view, forecast_function_name):
    """
    Train the Machine Learning Model in Snowflake
    """
    # Create a view to be used as input for the model
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS 
                          SELECT DATE, CLOSE, SYMBOL FROM {train_input_table};"""
    # SQL to create the forecasting model in Snowflake
    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""
    try:
        cursor.execute(create_view_sql)  # Create the view
        cursor.execute(create_model_sql)  # Create the model
        cursor.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")  # Display evaluation metrics
    except Exception as e:
        print(f"Error During Training: {e}")
        raise

# Task to predict future stock prices based on the trained model
@task
def predict(cursor, forecast_function_name, train_input_table, forecast_table, final_table):
    """
    Generate Predictions and Store Them in a Forecast Table
    """
    # SQL to generate predictions based on the trained model
    make_prediction_sql = f"""
    BEGIN
        CALL {forecast_function_name}!FORECAST(FORECASTING_PERIODS => 7, CONFIG_OBJECT => {{'prediction_interval': 0.95}});
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;
    """
    # SQL to create the final table with actual and forecasted stock prices
    create_final_table_sql = f"""
    CREATE OR REPLACE TABLE {final_table} AS
    SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
    FROM {train_input_table}
    UNION ALL
    SELECT REPLACE(series, '"', '') AS SYMBOL, ts AS DATE, NULL AS actual, forecast, lower_bound, upper_bound
    FROM {forecast_table};
    """
    try:
        cursor.execute(make_prediction_sql)  # Execute prediction
        cursor.execute(create_final_table_sql)  # Create final table
    except Exception as e:
        print(f"Error During Prediction: {e}")
        raise

# Define the Airflow DAG
with DAG(
    dag_id="FinanceDataAnalytics",
    default_args={
        "owner": "Pair 9",
        "email": ["shatayu.thakur@sjsu.edu", "pranav.raveendran@sjsu.edu"],
        "email_on_failure": True,
        "email_on_retry": True,
        "email_on_success": True,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    start_date=datetime(2024, 10, 9),
    catchup=False,
    tags=["ETL", "ML"],
    schedule_interval='0 */4 * * *',  # Run every 4 hours
) as dag:

    # Define table and symbol variables
    target_table = "dev.raw_data.stock_data"
    symbol_apple = "AAPL"
    symbol_nvidia = "NVDA"

    # Task 1: Initialize the target table in Snowflake
    initialize_task = initialize_target_table(target_table)

    # Task 2: Apple ETL Sequence
    apple_data = extract_stock_data(symbol_apple)  # Extract stock price data for Apple
    transformed_apple = transform_to_90d_stock_data(apple_data)  # Transform data to last 90 days
    load_apple = load_stock_data(target_table, transformed_apple, symbol_apple)  # Load Apple data into Snowflake

    # Task 3: NVIDIA ETL Sequence
    nvidia_data = extract_stock_data(symbol_nvidia)  # Extract stock price data for NVIDIA
    transformed_nvidia = transform_to_90d_stock_data(nvidia_data)  # Transform data to last 90 days
    load_nvidia = load_stock_data(target_table, transformed_nvidia, symbol_nvidia)  # Load NVIDIA data into Snowflake

    # Task 4: Machine Learning Prediction
    train_input_table = "dev.raw_data.stock_data"
    train_view = "dev.adhoc.stock_data_view"
    forecast_table = "dev.adhoc.stock_data_forecast"
    forecast_function_name = "dev.analytics.predict_stock_price"
    final_table = "dev.analytics.final_stock_data"
    cursor = return_snowflake_conn()

    # Train the model and predict stock prices
    train_task = train(cursor, train_input_table, train_view, forecast_function_name)
    predict_task = predict(cursor, forecast_function_name, train_input_table, forecast_table, final_table)

    # Define task dependencies
    initialize_task >> [load_apple, load_nvidia] >> train_task >> predict_task
