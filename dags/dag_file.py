from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from scripts.stock_utils import DataManager

manager = DataManager()

with DAG(
        dag_id="dag_dl_transfer_to_ch",
        schedule_interval="@daily",
        start_date=datetime(2026, 3, 1),
        catchup=True,
        max_active_runs=1,
) as dag:
    download_psql = PythonOperator(
        task_id="download_step",
        python_callable=lambda **kwargs: manager.download_and_save_to_pg(
            symbol="AAPL",
            start_date=kwargs['data_interval_start'],
            end_date=kwargs['data_interval_end'],
            ds=kwargs['ds']
        )
    )
    transfer_to_ch = PythonOperator(
        task_id="transfer_step",
        python_callable=lambda **kwargs: manager.transfer_to_clickhouse(kwargs['ds'])
    )

    download_psql >> transfer_to_ch

