# Data 226 - HW7: Weekly Active User (WAU) Chart Creation

## Overview

This repository contains the code and DAG configurations for creating a Weekly Active User (WAU) chart as part of the ETL process using **Airflow** and **Snowflake**. The project also includes the integration of **Preset** or **Docker Superset** for visualization purposes. The project involves the following key tasks:

1. **Importing Tables into Snowflake** using Airflow as part of an ETL DAG.
2. **Creating an ELT DAG** to join two tables (`user_session_channel` and `session_timestamp`) and create the `session_summary` table with duplicate record checks.
3. **Setting Up Preset/Docker Superset** and connecting to Snowflake for visualization.
4. **Creating the WAU Chart** in Preset/Docker Superset.

## Project Structure

- **`build_summary_dag.py`**: This DAG handles the process of creating the `session_summary` table by joining the `user_session_channel` and `session_timestamp` tables. The code includes a check for duplicate records to ensure only unique sessions are considered.
  
- **`session_to_snowflake_dag.py`**: This DAG loads the raw data tables (`user_session_channel` and `session_timestamp`) from a source into Snowflake under the `raw_data` schema.

## Instructions

### Step 1: Set Up the Snowflake Connection

Ensure that the `snowflake_conn_id` in both DAGs points to the correct Snowflake connection that is configured in your Airflow instance.

### Step 2: Run the DAGs

1. **Session to Snowflake DAG** (`session_to_snowflake_dag.py`): Run this DAG to load raw data into Snowflake.
2. **Build Summary DAG** (`build_summary_dag.py`): Run this DAG to create the `session_summary` table, which includes the logic for joining and deduplication.

### Step 3: Set Up Preset or Docker Superset

Connect Preset or Docker Superset to your Snowflake instance and import the `session_summary` table. Use this table to create a Weekly Active User (WAU) chart.

### Step 4: Create WAU Chart

In Preset or Docker Superset:
1. Import the `session_summary` dataset.
2. Create a new chart using the WAU metric.
3. Save and export the chart.

## Files

- [`build_summary_dag.py`](./build_summary_dag.py): Creates the `session_summary` table with deduplication.
- [`session_to_snowflake_dag.py`](./session_to_snowflake_dag.py): Loads raw data into Snowflake.

## Screenshots

- **DAG Screenshots**: Screenshots of the Airflow DAG’s detailed pages.
- **WAU Chart Screenshot**: Screenshot of the WAU chart created in Preset/Docker Superset.

## Repository Link

[GitHub Repository](https://github.com/pranavreve/Data226/tree/HW7)
