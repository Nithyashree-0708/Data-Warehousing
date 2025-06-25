from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# File path inside the container (Docker path)
ORDERS_FILE = '/opt/airflow/data/orders.csv'
REQUIRED_COLUMNS = ['OrderID', 'CustomerName', 'Product', 'Quantity', 'Price']
MANDATORY_FIELDS = ['OrderID', 'CustomerName', 'Product']

default_args = {
    'owner': 'nithyashree',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def validate_data():
    if not os.path.exists(ORDERS_FILE):
        raise FileNotFoundError(f"{ORDERS_FILE} not found.")

    df = pd.read_csv(ORDERS_FILE)
    
    # Step 2: Validate required columns
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Step 3: Check for nulls in mandatory fields
    for field in MANDATORY_FIELDS:
        if df[field].isnull().any():
            raise ValueError(f"Null values found in mandatory field: {field}")
    
    print(" Validation passed.")

def summarize_data():
    df = pd.read_csv(ORDERS_FILE)
    summary = df.groupby("Product")['Quantity'].sum()
    print(" Product-wise Quantity Summary:\n", summary)

with DAG(
    dag_id='assignment_2_data_quality_validation',
    default_args=default_args,
    description='Validate and summarize order data',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    validate = PythonOperator(
        task_id='validate_orders_csv',
        python_callable=validate_data
    )

    summarize = PythonOperator(
        task_id='summarize_valid_data',
        python_callable=summarize_data
    )

    validate >> summarize
