from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.exceptions import AirflowException
import os
import pandas as pd
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

with DAG(
        'data_processing_dag',
        default_args=default_args,
        description='A DAG to process website traffic data from a CSV file',
        start_date=datetime(2024, 5, 27),
        schedule_interval=None,
        tags=['data-processing']
) as dag:
    def process_csv_file(dag_id):
        # Construct the input file path using CUR_DIR
        input_filepath = os.path.join(CUR_DIR, 'data_set', 'Website_Logs.csv')

        if not os.path.exists(input_filepath):
            raise AirflowException(f"Input file does not exist: {input_filepath}")

        # Read the CSV file
        listings = pd.read_csv(input_filepath)

        # Add a new column for the DAG ID
        listings['dag_id'] = dag_id

        # Convert 'accessed_date' column to datetime
        listings['accessed_date'] = pd.to_datetime(listings['accessed_date'], errors='coerce')

        # Convert specified columns to integer type
        listings['duration_(secs)'] = pd.to_numeric(listings['duration_(secs)'], errors='coerce').fillna(0).astype(int)
        listings['age'] = pd.to_numeric(listings['age'], errors='coerce').fillna(0).astype(int)
        listings['sales'] = pd.to_numeric(listings['sales'], errors='coerce').fillna(0).astype(int)
        listings['returned_amount'] = pd.to_numeric(listings['returned_amount'], errors='coerce').fillna(0).astype(int)

        # Database connection details
        db_url = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'

        # Create SQLAlchemy engine
        engine = create_engine(db_url)

        # Save the DataFrame to the database using the engine
        listings.to_sql('web_logs_cleans', engine, if_exists='replace', index=False)

        return "File processed and data saved to PostgreSQL database"


    process_csv_task = PythonOperator(
        task_id='process_csv_file',
        python_callable=process_csv_file,
        op_args=[dag.dag_id],
    )

    process_csv_task
