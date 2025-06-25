from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import shutil

def summarize_sales():
    df = pd.read_csv("/opt/airflow/dags/data/sales.csv")
    summary = df.groupby("category")["amount"].sum().reset_index()
    summary.to_csv("/opt/airflow/dags/data/sales_summary.csv", index=False)

def archive_sales():
    shutil.move("/opt/airflow/dags/data/sales.csv", "/opt/airflow/dags/data/archive/sales.csv")

with DAG("assignment_2_daily_sales", schedule_interval="0 6 * * *", start_date=datetime(2023, 1, 1), catchup=False) as dag:
    t1 = PythonOperator(task_id="summarize_sales", python_callable=summarize_sales, execution_timeout=timedelta(minutes=5))
    t2 = PythonOperator(task_id="archive_sales", python_callable=archive_sales, execution_timeout=timedelta(minutes=5))
    t1 >> t2
