from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define the DAG
with DAG(
    dag_id='BuildSummary',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust the schedule as needed
    description='A DAG to build session summary in the analytics schema in Snowflake',
    catchup=False,
) as dag:

    # Task to ensure the analytics schema exists
    create_analytics_schema = SnowflakeOperator(
        task_id='create_analytics_schema',
        snowflake_conn_id='snowflake_conn',
        sql="""
        USE DATABASE dev;
        CREATE SCHEMA IF NOT EXISTS dev.analytics;
        """,
    )

    # Task to create a session summary table with a deduplication check in the analytics schema
    run_ctas = SnowflakeOperator(
        task_id='run_ctas',
        snowflake_conn_id='snowflake_conn',
        sql="""
        USE SCHEMA dev.analytics;

        CREATE TABLE IF NOT EXISTS dev.analytics.session_summary AS
        SELECT 
            usc.userId, 
            usc.sessionId, 
            usc.channel, 
            st.ts
        FROM 
            dev.raw_data.user_session_channel usc
        JOIN 
            dev.raw_data.session_timestamp st
        ON 
            usc.sessionId = st.sessionId
        WHERE 
            usc.sessionId IN (
                SELECT sessionId 
                FROM dev.raw_data.user_session_channel
                GROUP BY sessionId
                HAVING COUNT(sessionId) = 1  -- Only select unique sessionId
            );
        """,
    )

    # Set the task sequence: First create the schema, then run the CTAS query
    create_analytics_schema >> run_ctas
