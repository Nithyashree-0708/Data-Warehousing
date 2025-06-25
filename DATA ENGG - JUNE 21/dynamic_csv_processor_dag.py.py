from airflow.decorators import dag, task
from datetime import datetime
import os
import pandas as pd
import glob

@dag(schedule=None, start_date=datetime(2023, 1, 1), catchup=False)
def assignment_5_dynamic_files():

    @task
    def list_files():
        return glob.glob("/opt/airflow/dags/data/input/*.csv")

    @task
    def validate(file_path: str):
        df = pd.read_csv(file_path)
        if not {"id", "name", "value"}.issubset(df.columns):
            raise ValueError(f"Invalid headers in {file_path}")
        return file_path

    @task
    def count_rows(file_path: str):
        df = pd.read_csv(file_path)
        return {"file": os.path.basename(file_path), "count": len(df)}

    @task
    def write_summaries(results: list):
        with open("/opt/airflow/dags/data/output_summary.csv", "w") as f:
            f.write("file,count\n")
            for r in results:
                f.write(f"{r['file']},{r['count']}\n")

    files = list_files()
    valid = validate.expand(file_path=files)
    results = count_rows.expand(file_path=valid)
    write_summaries(results)

assignment_5_dynamic_files()
