from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define the DAG
with DAG(
    dag_id='SessionToSnowflake',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    description='ETL process to load data into Snowflake',
    catchup=False
) as dag:

    # Task to set up the Snowflake stage (for loading files from S3)
    set_stage = SnowflakeOperator(
        task_id='set_stage',
        snowflake_conn_id='snowflake_conn',
        sql="""
        CREATE OR REPLACE STAGE dev.raw_data.blob_stage
        url = 's3://s3-geospatial/readonly/'
        file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
        """,
    )

    # Task to load data into user_session_channel table
    load_user_session_channel = SnowflakeOperator(
        task_id='load_user_session_channel',
        snowflake_conn_id='snowflake_conn',
        sql='COPY INTO dev.raw_data.user_session_channel FROM @dev.raw_data.blob_stage/user_session_channel.csv;',
    )

    # Task to load data into session_timestamp table
    load_session_timestamp = SnowflakeOperator(
        task_id='load_session_timestamp',
        snowflake_conn_id='snowflake_conn',
        sql='COPY INTO dev.raw_data.session_timestamp FROM @dev.raw_data.blob_stage/session_timestamp.csv;',
    )

    # Task dependencies
    set_stage >> load_user_session_channel >> load_session_timestamp
