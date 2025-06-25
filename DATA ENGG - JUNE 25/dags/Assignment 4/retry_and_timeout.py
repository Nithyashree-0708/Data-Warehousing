from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import random

default_args = {
    'owner': 'nithyashree',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

def simulate_work():
    sleep_time = random.choice([5, 10, 15])
    print(f" Simulating work... will sleep for {sleep_time} seconds.")
    time.sleep(sleep_time)
    
    if sleep_time > 10:
        print(" Task took too long... raising error.")
        raise Exception("Simulated task exceeded acceptable execution time.")
    else:
        print(" Task completed within acceptable time.")

with DAG(
    dag_id='assignment_4_retry_timeout_handling',
    default_args=default_args,
    description='Simulates long-running flaky task with retry and timeout',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    long_task = PythonOperator(
        task_id='flaky_long_task',
        python_callable=simulate_work,
        execution_timeout=timedelta(seconds=12),
        retry_exponential_backoff=True
    )
