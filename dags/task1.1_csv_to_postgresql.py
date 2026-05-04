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

LOAD_CONFIGS = [
    {
        'table_name': 'ds.ft_balance_f',
        'primary_key': ['on_date', 'account_rk'],
        'date_columns': {'ON_DATE': '%d.%m.%Y'},
    },
    {
        'table_name': 'ds.ft_posting_f',
        'primary_key': None,
        'date_columns': {'OPER_DATE': '%d-%m-%Y'},
    },
    {
        'table_name': 'ds.md_account_d',
        'primary_key': ['data_actual_date', 'account_rk'],
        'date_columns': {'DATA_ACTUAL_DATE': '%Y-%m-%d', 'DATA_ACTUAL_END_DATE': '%Y-%m-%d'}
    },
    {
        'table_name': 'ds.md_currency_d',
        'primary_key': ['currency_rk', 'data_actual_date'],
        'date_columns': {'DATA_ACTUAL_DATE': '%Y-%m-%d', 'DATA_ACTUAL_END_DATE': '%Y-%m-%d'}
    },
    {
        'table_name': 'ds.md_exchange_rate_d',
        'primary_key': ['data_actual_date', 'currency_rk'],
        'date_columns': {'DATA_ACTUAL_DATE': '%Y-%m-%d', 'DATA_ACTUAL_END_DATE': '%Y-%m-%d'}
    },
    {
        'table_name': 'ds.md_ledger_account_s',
        'primary_key': ['ledger_account', 'start_date'],
        'date_columns': {'START_DATE': '%Y-%m-%d', 'END_DATE': '%Y-%m-%d'}
    }
]

def start_func():
    logger.info("DAG started")

def load_csv_to_postgres(file_config, **context):
    """Загрузка данных из CSV в PostgreSQL"""
    
    table_name = file_config['table_name']
    csv_path = f"/opt/airflow/data/{table_name.split('.')[-1]}.csv"
    primary_key = file_config.get('primary_key', [])
    date_columns = file_config.get('date_columns', {})

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    start_time = datetime.now()
    error_message = None
    status = 'SUCCESS'
    rows_affected = 0
    
    try:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Файл не найден: {csv_path}")
        
        df = None
        for enc in ['utf-8', 'latin1']:
            try:
                df = pd.read_csv(csv_path, sep=';', encoding=enc)
                logger.info(f"Файл {csv_path} прочитан с кодировкой {enc}")
                break
            except UnicodeDecodeError:
                continue
        else:
            raise UnicodeDecodeError(f"Не удалось прочитать файл {csv_path} ни в одной из кодировок")
        
        target_columns = list(df.columns)
        logger.info(f"Найдены колонки в файле: {target_columns}")
        logger.info(f"Прочитано {len(df)} строк из {csv_path}")

        # Преобразование дат
        for col, date_format in date_columns.items():
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format=date_format, errors='coerce').dt.date

        # Форматирование CURRENCY_CODE
        if 'CURRENCY_CODE' in df.columns:
            df['CURRENCY_CODE'] = df['CURRENCY_CODE'].apply(
                lambda x: f"{int(float(x)):03d}" if pd.notnull(x) and str(x).replace('.', '').replace('-', '').isdigit() 
                else str(x)[:3] if pd.notnull(x) and str(x) 
                else None
            )
            logger.info("CURRENCY_CODE обработан")
        

        records = [tuple(None if pd.isna(val) else val for val in row[1:]) for row in df[target_columns].itertuples()]
        rows_affected = len(records)
        
        logger.info(f"Ожидание 5 секунд перед загрузкой...")
        #time.sleep(5)
        logger.info(f"Продолжение загрузки")

        if primary_key is None:
            pg_hook.run(f"TRUNCATE TABLE {table_name} CASCADE")
            
            pg_hook.insert_rows(
                table=table_name,
                rows=records,
                target_fields=target_columns,
                replace=False
            )

            logger.info(f"Таблица {table_name}: загружено {rows_affected} записей (TRUNCATE + INSERT)")
            
        else:  # upsert
            
            conflict_target = ', '.join(primary_key)
            set_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in target_columns if col not in primary_key])
            
            for record in records:
                upsert_sql = f"""
                INSERT INTO {table_name} ({', '.join(target_columns)})
                VALUES ({', '.join(['%s'] * len(target_columns))})
                ON CONFLICT ({conflict_target}) 
                DO UPDATE SET {set_clause}
                """
                pg_hook.run(upsert_sql, parameters=record)
            
            logger.info(f"Таблица {table_name}: обновлено {rows_affected} записей (UPSERT)")
            
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
    dag_id='task1.1_csv_to_postgresql_dag',
    default_args=default_args,
    description='Загрузка CSV в PostgreSQL',
    schedule='@once',
    catchup=False,
    tags=['etl', 'csv', 'simple'],
) as dag:
    
    start = PythonOperator(
    task_id="start",
    python_callable=start_func
    )
    
    load_tasks = []
    
    for config in LOAD_CONFIGS:
        load_task = PythonOperator(
            task_id=f'load_{config["table_name"]}',
            python_callable=load_csv_to_postgres,
            op_kwargs={'file_config': config}
        )
        load_tasks.append(load_task)

    start >> load_tasks