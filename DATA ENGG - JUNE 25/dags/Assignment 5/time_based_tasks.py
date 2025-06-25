from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pendulum

default_args = {
    'owner': 'nithyashree',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def decide_which_task():
    now = pendulum.now("Asia/Kolkata")  
    current_hour = now.hour
    weekday = now.weekday()  # Monday=0, Sunday=6

    print(f" Current time: {now}, Hour: {current_hour}, Weekday: {weekday}")

    if weekday >= 5:
        print(" It's the weekend. Skipping the DAG.")
        return "skip_dag"

    if current_hour < 12:
        return "run_morning_task"
    else:
        return "run_afternoon_task"

def morning_task():
    print(" Running morning task!")

def afternoon_task():
    print(" Running afternoon task!")

def cleanup():
    print(" Final cleanup task executed.")

with DAG(
    dag_id="assignment_5_time_based_branching",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=None,
    description="Conditional DAG based on time of day and day of week"
) as dag:

    check_time = BranchPythonOperator(
        task_id='check_time_and_decide',
        python_callable=decide_which_task
    )

    morning = PythonOperator(
        task_id='run_morning_task',
        python_callable=morning_task
    )

    afternoon = PythonOperator(
        task_id='run_afternoon_task',
        python_callable=afternoon_task
    )

    skip = DummyOperator(task_id='skip_dag')

    final_cleanup = PythonOperator(
        task_id='cleanup_task',
        python_callable=cleanup,
        trigger_rule='none_failed_min_one_success'
    )

    check_time >> [morning, afternoon, skip]
    [morning, afternoon, skip] >> final_cleanup
