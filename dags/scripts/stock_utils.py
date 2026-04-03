import yfinance as yf
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.exceptions import AirflowSkipException

class DataManager:
    def __init__(self, pg_conn_id='my_postgres', ch_conn_id='clickhouse'):
        self.pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
        self.ch_hook = ClickHouseHook(clickhouse_conn_id=ch_conn_id)

    def download_and_save_to_pg(self, symbol, start_date, end_date, ds):

        data = yf.download(symbol, start=start_date, end=end_date)

        if data.empty:
            raise AirflowSkipException(f"No data for {ds}")

        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        data = data.reset_index()
        data.columns = [col.lower().replace(' ', '_') for col in data.columns]


        insert_sql = """
            INSERT INTO stock_data_table_yfinance (date, open, high, low, close, adj_close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING;
        """

        for _, row in data.iterrows():
            params = (
                str(row['date']),
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row.get('adj_close', row['close']),
                row['volume']
            )
            self.pg_hook.run(insert_sql, parameters=params)


    def transfer_to_clickhouse(self, execution_date):
        sql = f"""
            INSERT INTO clickhouse.stock_data_final
            SELECT * FROM default.pg_data_yfinance
        """
        self.ch_hook.execute(sql)

