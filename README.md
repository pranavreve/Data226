# Yfinance to Snowflake DAG

This Airflow DAG downloads stock price data from Yahoo Finance and loads it into a Snowflake table. The DAG uses the `yfinance` library to extract the stock data for a given symbol and inserts the data into a specified Snowflake table.

## Overview

The DAG performs the following steps:
1. **Extract**: Download stock price data from Yahoo Finance for the symbol `AAPL` (Apple Inc.) for the current day.
2. **Load**: Insert the stock data into the Snowflake table `dev.raw_data.stock_price`.

### DAG Details

- **DAG ID**: `YfinanceToSnowflake`
- **Schedule**: Runs daily at 2:30 AM UTC.
- **Tasks**:
  - `extract`: Downloads stock data using the `yfinance` library.
  - `load`: Loads the extracted data into a Snowflake table using the `SnowflakeHook` and Snowflake SQL.

## Requirements

- **Airflow Version**: 2.x or above
- **Dependencies**: 
  - `yfinance` (for downloading stock data from Yahoo Finance)
  - `snowflake-connector-python` (for connecting to Snowflake)
  - Airflow `SnowflakeHook` for Snowflake operations.
  
### Airflow Connection

You must configure an Airflow connection to Snowflake before running the DAG. Here's how to do it:

1. Go to the Airflow UI.
2. Navigate to **Admin → Connections**.
3. Create a new connection with the following details:
    - **Conn ID**: `snowflake_conn`
    - **Conn Type**: `Snowflake`
    - **Account**: Your Snowflake account identifier (e.g., `MBB29368`).
    - **User**: Your Snowflake user (e.g., `pranavrevee`).
    - **Password**: Your Snowflake password.
    - **Warehouse**: The Snowflake warehouse to run queries (e.g., `COMPUTE_WH`).
    - **Database**: The target Snowflake database (e.g., `dev`).
    - **Schema**: The target Snowflake schema (e.g., `raw_data`).
    - **Role**: (Optional) Role used to access Snowflake (e.g., `ACCOUNTADMIN`).

## Setup Instructions

1. Clone the repository:
    ```bash
    git clone https://github.com/your-username/your-repo.git
    cd your-repo
    ```

2. Place the DAG file (`yfinance_to_snowflake.py`) in the `dags/` folder of your Airflow environment.

3. Install necessary Python libraries:
    ```bash
    pip install yfinance snowflake-connector-python
    ```

4. Ensure the Snowflake connection is configured in Airflow under **Admin → Connections** with the Conn ID `snowflake_conn`.

## DAG Code

```python
# (include your code block here)
