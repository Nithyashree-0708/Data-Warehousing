from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random

def flaky_api():
    if random.random() < 0.5:
        raise Exception("Random failure")
    print("API succeeded")

def send_alert(context):
    print("All retries failed!")

def follow_up():
    print("This runs only if API succeeded.")

with DAG("assignment_3_retry_alert", start_date=datetime(2023, 1, 1), schedule=None, catchup=False) as dag:
    t1 = PythonOperator(
        task_id="flaky_api",
        python_callable=flaky_api,
        retries=3,
        retry_delay=timedelta(seconds=15),
        retry_exponential_backoff=True,
        on_failure_callback=send_alert
    )
    t2 = PythonOperator(task_id="follow_up", python_callable=follow_up, trigger_rule="all_success")
    t1 >> t2
