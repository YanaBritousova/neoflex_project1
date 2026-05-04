from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extras import execute_values
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)

def start_func():
    logger.info("DAG started")


def calculate_form101_for_january(**context):
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    start_time = datetime.now()
    error_message = None
    status = 'SUCCESS'
    table_name = "dm.dm_f101_round_f"
    
    current_date = date(2018,2,1)

    try:
        
        date_str = current_date.strftime('%Y-%m-%d')
        logger.info(f"Расчет формы 101")
            
        sql = "CALL dm.fill_f101_round_f(%s)"
        pg_hook.run(sql, parameters=(current_date,))
            
        logger.info(f"Успешно рассчитано")
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
    dag_id='task1.3_calc_form101_dag',
    default_args=default_args,
    schedule='@once',
    catchup=False,
) as dag:
    
    start = PythonOperator(
        task_id = "start",
        python_callable=start_func
    )
    
    calculate_form101 = PythonOperator(
        task_id='calculate_form101_for_january_2018',
        python_callable=calculate_form101_for_january,
        trigger_rule='all_success'
    )

start >> calculate_form101
