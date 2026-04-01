from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from datetime import datetime


def run_query_via_hook():
    # Инициализируем хук, используя созданное ранее подключение
    hook = ClickHouseHook(clickhouse_conn_id='clickhouse')

    # Выполняем запрос. По умолчанию в ClickHouse логи лежат в system.query_log
    sql = "SELECT 0"

    # Метод .run() возвращает список кортежей
    result = hook.execute(sql = sql)

    print("--- Результаты запроса из ClickHouse ---")
    for row in result:
        print(row)


with DAG(
        dag_id="clickhouse_hook_test",
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False
) as dag:
    task_run_hook = PythonOperator(
        task_id="test_hook_connection",
        python_callable=run_query_via_hook
    )

    transfer_to_ch = ClickHouseOperator(
        task_id="transfer_from_pg_to_ch",
        clickhouse_conn_id="clickhouse",
        sql="""
            INSERT INTO clickhouse.stock_data_final
            SELECT * FROM default.pg_data_yfinance
            """
    )

    task_run_hook >> transfer_to_ch
