from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'nithyashree',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def parent_task():
    print(" Parent DAG task executed.")

with DAG(
    dag_id='assignment_3_parent_dag',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description='Parent DAG that triggers child DAG with metadata'
) as dag:

    task1 = PythonOperator(
        task_id='run_parent_task',
        python_callable=parent_task
    )

    trigger_child = TriggerDagRunOperator(
        task_id='trigger_child_dag',
        trigger_dag_id='assignment_3_child_dag',
        conf={"triggered_at": str(datetime.now())}
    )

    task1 >> trigger_child
