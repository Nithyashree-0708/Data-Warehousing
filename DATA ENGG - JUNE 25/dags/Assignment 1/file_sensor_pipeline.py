from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import shutil
import os

# Paths inside the container
FILE_PATH = "/opt/airflow/data/incoming/report.csv"
ARCHIVE_PATH = "/opt/airflow/data/archive/report.csv"

default_args = {
    'owner': 'nithyashree',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define processing logic
def process_report():
    df = pd.read_csv(FILE_PATH)
    print(" File found and read successfully:")
    print(df.head())

def move_file():
    os.makedirs(os.path.dirname(ARCHIVE_PATH), exist_ok=True)
    shutil.move(FILE_PATH, ARCHIVE_PATH)
    print(f" File moved to archive: {ARCHIVE_PATH}")

with DAG(
    dag_id='assignment_1_file_sensor_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description='Waits for report.csv and processes it.'
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_report_csv',
        filepath=FILE_PATH,
        fs_conn_id='fs_default',
        poke_interval=30,
        timeout=600,
        mode='poke'
    )

    process_task = PythonOperator(
        task_id='process_report_file',
        python_callable=process_report
    )

    archive_task = PythonOperator(
        task_id='move_to_archive',
        python_callable=move_file
    )

    wait_for_file >> process_task >> archive_task
