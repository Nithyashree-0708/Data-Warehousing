from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import random

default_args = {
    'owner': 'nithyashree',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': [Variable.get("alert_email", default_var="nithyashreer2019@gmail.com")],
}

def task_1_logic():
    print(" Task 1 completed.")

def task_2_logic():
    print(" Task 2 may randomly fail...")
    if random.choice([True, False]):
        raise Exception(" Task 2 failed intentionally.")
    print(" Task 2 succeeded.")

with DAG(
    dag_id='assignment_6_email_notification',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=None,
    description="Send success/failure emails based on task outcome"
) as dag:

    task1 = PythonOperator(
        task_id='task_one',
        python_callable=task_1_logic
    )

    task2 = PythonOperator(
        task_id='task_two',
        python_callable=task_2_logic
    )

    success_email = EmailOperator(
        task_id='send_success_email',
        to=Variable.get("alert_email", default_var="nithyashreer2019l@gmail.com"),
        subject=" Airflow DAG Success: assignment_6_email_notification",
        html_content="<h3>All tasks completed successfully!</h3>",
        trigger_rule='all_success'  # Ensures it's only sent if everything passed
    )

    [task1, task2] >> success_email
