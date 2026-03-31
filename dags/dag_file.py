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
    data = yf.download("AAPL", start=context['data_interval_start'], end=context['data_interval_end'])

    if data.empty:
        raise AirflowSkipException(f"No data for {context['ds']}")

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    data = data.reset_index()

    data.columns = [col.lower().replace(' ', '_') for col in data.columns]

    hook = PostgresHook(postgres_conn_id='my_postgres')

    for _, row in data.iterrows():
        insert_sql = """
              INSERT INTO stock_data_table_yfinance (date, open, high, low, close, adj_close, volume)
              VALUES (%s, %s, %s, %s, %s, %s, %s)
          """

        params = (
            str(row['date']),
            row['open'],
            row['high'],
            row['low'],
            row['close'],
            row.get('adj_close', row['close']),
            row['volume']
        )
        hook.run(insert_sql, parameters=params)

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

