from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)
def start_func():
    logger.info("DAG started")

def export_to_csv(**context):

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    output_file = f"/opt/airflow/exports/dm_f101_round_f_{datetime.now().strftime('%Y%m%d')}.csv"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    sql = "SELECT * FROM dm.dm_f101_round_f ORDER BY from_date, ledger_account"
    df = pg_hook.get_pandas_df(sql)
    df.to_csv(output_file, sep=';', index=False, encoding='utf-8-sig')
    
    
with DAG(
    dag_id='task1.4.1_form101_to_csv_dag',
    schedule='@once',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    start = PythonOperator(
        task_id = "start",
        python_callable=start_func
    )
    
    export_task = PythonOperator(
        task_id='export_to_csv',
        python_callable=export_to_csv
    )
start >> export_task