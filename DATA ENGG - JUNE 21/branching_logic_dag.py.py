from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os

def check_file_size():
    size = os.path.getsize("/opt/airflow/dags/data/inventory.csv")
    return "task_a" if size < 500_000 else "task_b"

def task_a():
    print("Running light summary")

def task_b():
    print("Running detailed processing")

with DAG("assignment_4_branching", start_date=datetime(2023, 1, 1), schedule=None, catchup=False) as dag:
    branch = BranchPythonOperator(task_id="branch_logic", python_callable=check_file_size)
    a = PythonOperator(task_id="task_a", python_callable=task_a)
    b = PythonOperator(task_id="task_b", python_callable=task_b)
    cleanup = EmptyOperator(task_id="cleanup", trigger_rule="none_failed_min_one_success")
    branch >> [a, b] >> cleanup
