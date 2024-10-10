# Alpha Vantage to Snowflake Data Pipeline

This repository contains an Apache Airflow DAG that integrates the Alpha Vantage API to fetch daily stock prices and stores the data into Snowflake.

## Overview

The DAG fetches the last 90 days of stock price data for a specific stock symbol (e.g., NVDA) from Alpha Vantage. The data is then inserted or updated in a Snowflake table, and the pipeline checks how many records exist in the Snowflake table.

### Key Features:
- Retrieves stock price data using Alpha Vantage's API.
- Loads the data into a Snowflake database.
- Automatically checks and logs the total number of records in the table after loading data.

## Requirements

You will need the following dependencies installed on your system:
- Python 3.x
- Apache Airflow
- Snowflake account
- Alpha Vantage API Key
- Required Python packages (see below)

## Setup Instructions

### Step 1: Install Apache Airflow

First, install Apache Airflow if it's not already installed. You can install it using `pip`:

```bash
pip install apache-airflow
