from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import pandas as pd

def check_file():
    if not os.path.exists("/opt/airflow/dags/data/customers.csv"):
        raise FileNotFoundError("customers.csv not found")

def count_rows():
    df = pd.read_csv("/opt/airflow/dags/data/customers.csv")
    return len(df)

def log_count(ti):
    count = ti.xcom_pull(task_ids="count_rows")
    print(f"Total rows: {count}")

def branch_on_count(ti):
    count = ti.xcom_pull(task_ids="count_rows")
    return "send_alert" if count > 100 else "no_alert"

with DAG("assignment_1_csv_to_summary", start_date=datetime(2023, 1, 1), schedule=None, catchup=False) as dag:
    t1 = PythonOperator(task_id="check_file", python_callable=check_file)
    t2 = PythonOperator(task_id="count_rows", python_callable=count_rows)
    t3 = PythonOperator(task_id="log_count", python_callable=log_count)
    branch = BranchPythonOperator(task_id="branch", python_callable=branch_on_count)
    
    alert = BashOperator(task_id="send_alert", bash_command='echo "Row count > 100!"')
    no_alert = BashOperator(task_id="no_alert", bash_command='echo "Row count <= 100"')

    t1 >> t2 >> t3 >> branch >> [alert, no_alert]

dag = dag
