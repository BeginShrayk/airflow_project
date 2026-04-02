from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
import yfinance as yf
from datetime import datetime
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
              ON CONFLICT (date) DO NOTHING;
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
        dag_id="dag_dl_transfer_to_ch",
        schedule_interval="@daily",
        start_date=datetime(2026, 3, 1),
        catchup=True,
        max_active_runs=1,
) as dag:
    download_psql = PythonOperator(
        task_id="download_psql",
        python_callable=download_data,
    )
    transfer_to_ch = ClickHouseOperator(
        task_id="transfer_from_pg_to_ch",
        clickhouse_conn_id="clickhouse",
        sql="""
                INSERT INTO clickhouse.stock_data_final
                SELECT * FROM default.pg_data_yfinance
                """
    )

    download_psql >> transfer_to_ch

