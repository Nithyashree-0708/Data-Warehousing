from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'nithyashree',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def child_task(**context):
    conf = context.get("dag_run").conf or {}
    triggered_at = conf.get("triggered_at", "No metadata received")
    print(f" Child DAG triggered by parent at: {triggered_at}")

with DAG(
    dag_id='assignment_3_child_dag',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description='Child DAG triggered externally by parent DAG'
) as dag:

    run_child = PythonOperator(
        task_id='run_child_task',
        python_callable=child_task,
        provide_context=True
    )
