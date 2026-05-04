from datetime import datetime, timedelta
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extras import execute_values
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import os

logger = logging.getLogger(__name__)

def start_func():
    logger.info("DAG started")

def load_form101_to_postgres(file_name, **context):

    table_name = "dm.dm_f101_round_f_v2"
    csv_path = f"/opt/airflow/exports/{file_name}"
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    start_time = datetime.now()
    error_message = None
    status = 'SUCCESS'
    rows_affected = 0
    
    try:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Файл не найден: {csv_path}")
        
        df = pd.read_csv(csv_path, sep=';')
        logger.info(f"Файл {csv_path} прочитан")
                
        target_columns = list(df.columns)
        logger.info(f"Найдены колонки в файле: {target_columns}")
        logger.info(f"Прочитано {len(df)} строк из {csv_path}")

        
        records = [tuple(None if pd.isna(val) else val for val in row) for row in df[target_columns].itertuples(index=False)]
        rows_affected = len(records)

        pg_hook.run(f"TRUNCATE TABLE {table_name} CASCADE")
            
        pg_hook.insert_rows(
            table=table_name,
            rows=records,
            target_fields=target_columns,
            replace=False
        )

        logger.info(f"Таблица {table_name}: загружено {rows_affected} записей")
            
            
    except Exception as e:
        status = 'FAILED'
        error_message = str(e)
        logger.error(f"Ошибка при загрузке {csv_path}: {error_message}")
        raise
    finally:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        log_sql = """
        INSERT INTO logs.logs 
            (affected_table_name, start_timestamp, end_timestamp, duration, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        pg_hook.run(log_sql, parameters=(
            table_name, start_time, end_time, duration, status, error_message
        ))


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='task1.4.2_form101_to_postgresql_dag',
    default_args=default_args,
    schedule='@once',
    catchup=False,
) as dag:
    
    start = PythonOperator(
    task_id="start",
    python_callable=start_func
    )
    
    
    import_task = PythonOperator(
            task_id="load_form101_to_postgres",
            python_callable=load_form101_to_postgres,
            op_kwargs={"file_name": "dm_f101_round_f_20260504.csv"}
        )

    start >> import_task