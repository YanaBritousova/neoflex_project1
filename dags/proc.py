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

def calculate_turnover_for_january(**context):
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    start_time = datetime.now()
    error_message = None
    status = 'SUCCESS'
    table_name = "dm.dm_account_turnover_f"
    
    start_date = datetime(2018, 1, 1)
    end_date = datetime(2018, 1, 31)
    current_date = start_date

    try:
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            logger.info(f"Расчет витрины за {date_str}")
            
            sql = "CALL ds.fill_account_turnover_f(%s::timestamp)"
            pg_hook.run(sql, parameters=(current_date,))
            
            logger.info(f"Успешно рассчитано за {date_str}")
            current_date += timedelta(days=1)
            
    except Exception as e:
        status = 'FAILED'
        error_message = str(e)
        logger.error(f"Ошибка при расчете за {current_date.strftime('%Y-%m-%d')}: {error_message}")
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
    'start_date': datetime(2024, 1, 1),  # обязательный параметр
    'retries': 1,
}

with DAG(
    dag_id='proc_dag',
    default_args=default_args,
    schedule='@once',
    catchup=False,
) as dag:
    
    start = PythonOperator(
        task_id = "start",
        python_callable=start_func
    )
    
    calculate_turnover = PythonOperator(
        task_id='calculate_turnover_for_january_2018',
        python_callable=calculate_turnover_for_january,
        trigger_rule='all_success'
    )

start >> calculate_turnover