from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from airflow.exceptions import AirflowException
import os
import pandas as pd
from sqlalchemy import create_engine
import logging

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

# Get the current directory
CUR_DIR = os.path.abspath(os.path.dirname(__file__))

# Define a logger
logger = logging.getLogger(__name__)

# Define the DAG
with DAG(
        'data_processing_dag',
        default_args=default_args,
        description='A DAG to process website traffic data from a CSV file',
        start_date=datetime(2024, 5, 27),
        schedule_interval=None,
        tags=['data-processing']
) as dag:
    # Define the function to process the CSV file
    def process_csv_file(run_id, dag_id, schema_name):
        try:
            # Construct the input file path using CUR_DIR
            input_filepath = os.path.join(CUR_DIR, 'data_setss', 'Website_Logs.csv')

            if not os.path.exists(input_filepath):
                raise AirflowException(f"Input file does not exist: {input_filepath}")

            # Read the CSV file
            listings = pd.read_csv(input_filepath)

            # Add a new column for the DAG ID
            listings['dag_id'] = dag_id

            # Add a new column for the run ID
            listings['run_id'] = run_id

            # Convert 'accessed_date' column to datetime
            listings['accessed_date'] = pd.to_datetime(listings['accessed_date'], errors='coerce')

            # Convert specified columns to integer type
            listings['duration_(secs)'] = pd.to_numeric(listings['duration_(secs)'], errors='coerce').fillna(0).astype(int)
            listings['age'] = pd.to_numeric(listings['age'], errors='coerce').fillna(0).astype(int)
            listings['sales'] = pd.to_numeric(listings['sales'], errors='coerce').fillna(0).astype(int)
            listings['returned_amount'] = pd.to_numeric(listings['returned_amount'], errors='coerce').fillna(0).astype(
                int)

            # Database connection details
            db_url = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'

            # Create SQLAlchemy engine
            engine = create_engine(db_url)

            # Save the DataFrame to the database using the engine
            listings.to_sql('web_logs_cleans', engine, if_exists='replace', index=False, schema=schema_name)

            return "File processed and data saved to PostgreSQL database"

        except AirflowException as ae:
            # Log and raise Airflow exceptions
            logger.error(str(ae))
            raise
        except Exception as e:
            # Log unexpected exceptions
            logger.error(f"Unexpected error: {e}")
            raise AirflowException("An unexpected error occurred during processing")


    # Define the PythonOperator task
    process_csv_task = PythonOperator(
        task_id='process_csv_file',
        python_callable=process_csv_file,
        op_kwargs={'run_id': '{{ run_id }}', 'dag_id': dag.dag_id, 'schema_name': 'myeg'},
    )

    create_run_table = PostgresOperator(
        task_id='create_run_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists myeg.dag_runs (
                dt date,
                dag_id character varying,
                run_id character varying,
                run_status string
            )
        """
    )

    update_run_table_success = PostgresOperator(
        task_id='update_run_table_success',
        postgres_conn_id='postgres_localhost',
        sql="""
            insert into myeg.dag_runs (dt, dag_id, run_id, run_status) values ('{{ ds }}', '{{ dag.dag_id }}', '{{ run_id }}', 'success')
        """
    )

    update_run_table_failed = PostgresOperator(
        task_id='update_run_table_failed',
        postgres_conn_id='postgres_localhost',
        sql="""
            insert into myeg.dag_runs (dt, dag_id, run_id, run_status) values ('{{ ds }}', '{{ dag.dag_id }}', '{{ run_id }}', 'failed')
        """,
        trigger_rule='all_failed'
    )

    # Add the tasks to the DAG
    process_csv_task >> create_run_table >> update_run_table_success
    process_csv_task >> update_run_table_failed
