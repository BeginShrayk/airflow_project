from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import yfinance as yf
from datetime import datetime
import os
import json

# сделать загрузку данных из yfinance в postgres


# дописать функцию загрузки данных в json.файл, создать папку джсон куда будут вливаться данные с аирфлоу

def download_data(**context):
    folder_path = os.path.join(os.environ.get('AIRFLOW_HOME', '.'), 'folder_json_files')
    data = yf.download("AAPL", start=context['data_interval_start'], end=context['data_interval_end'])
    file_name = f"aapl_data_{context['ds']}.json"
    full_path = os.path.join(folder_path, file_name)
    data.to_json(full_path, orient='records', date_format='iso')



with DAG(
        dag_id="my_dag",
        schedule_interval="@daily",
        start_date=datetime(2026, 3, 1),
        catchup=True,
        max_active_runs=1,
) as dag:
    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=download_data,
    )

