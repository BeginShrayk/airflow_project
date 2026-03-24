from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException
import yfinance as yf
from datetime import datetime
import os
import json
import pandas as pd

def download_data(**context):
    # Скачиваем данные
    data = yf.download("AAPL", start=context['data_interval_start'], end=context['data_interval_end'])

    if data.empty:
        print(f"Данных за {context['ds']} нет. Пропускаем.")
        raise AirflowSkipException(f"No data for {context['ds']}")

    df = data.copy()  #  Обработка данных, если они есть

    if isinstance(df.columns, pd.MultiIndex): # Исправляем MultiIndex, если он есть
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index() # Подготовка данных: сбрасываем индекс, чтобы Date стала колонкой

    records = df.to_dict(orient='records') # Превращаем типы данных Pandas/Numpy в стандартные типы Python

    hook = PostgresHook(postgres_conn_id='my_postgres') # Подключаемся к Postgres

    for row in records: # Загрузка в Postgres
        json_row = json.dumps(row, default=str)
        insert_sql = "INSERT INTO stock_json_table (data) VALUES (%s)"
        hook.run(insert_sql, parameters=(json_row,))


with DAG(
        dag_id="my_dag",
        schedule_interval="@daily",
        start_date=datetime(2024, 1, 1),
        catchup=True,
        max_active_runs=1,
) as dag:
    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=download_data,
    )

