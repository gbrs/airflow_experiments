from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

import logging

from datetime import datetime, timedelta
import pandas as pd
import sqlite3

# настройка логгера
logger = logging.getLogger(__name__)

# настройка DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# описываем DAG
@dag(
    dag_id='modern_etl_pipeline',
    default_args=default_args,
    description='Современный ETL пайплайн с использованием Airflow 2.0+',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'example'],
    max_active_runs=1,
)
def modern_etl_pipeline():

    # используем TaskGroup для организации задач. Зачем он если у нас одна группа?
    @task_group(group_id='etl_process')
    def run_etl():
        
        # таска для Extract
        @task(task_id='extract')
        def extract():
            try:
                input_path = Variable.get("INPUT_CSV_PATH", default_var='/path/to/input_data.csv')
                logger.info(f"Extracting data from {input_path}")
                
                df = pd.read_csv(input_path)
                return df.to_dict('records')  # Возвращаем данные через XCom
                
            except Exception as e:
                logger.error(f"Extraction failed: {str(e)}")
                raise

        # Transform
        @task(task_id='transform')
        def transform(data: list):
            try:
                logger.info("Transforming data")
                df = pd.DataFrame.from_records(data)
                
                # Пример преобразований
                if 'name' in df.columns:
                    df['name'] = df['name'].str.upper()
                
                if all(col in df.columns for col in ['quantity', 'price']):
                    df['total'] = df['quantity'] * df['price']
                
                return df.to_dict('records')
                
            except Exception as e:
                logger.error(f"Transformation failed: {str(e)}")
                raise

        # Load
        @task(task_id='load')
        def load(data: list):
            try:
                db_path = Variable.get("SQLITE_DB_PATH", default_var='/path/to/database.db')
                logger.info(f"Loading data to SQLite at {db_path}")
                
                df = pd.DataFrame.from_records(data)
                conn = sqlite3.connect(db_path)
                
                # Используем контекстный менеджер для соединения
                with conn:
                    df.to_sql(
                        'processed_data', 
                        conn, 
                        if_exists='append', 
                        index=False,
                        chunksize=1000  # Пакетная вставка для больших данных
                    )
                
                logger.info(f"Successfully loaded {len(df)} records")
                
            except Exception as e:
                logger.error(f"Loading failed: {str(e)}")
                raise
                # можно добавить автоматическую очистку при ошибке

        # организация потока задач с использованием XCom
        raw_data = extract()
        transformed_data = transform(raw_data)
        load(transformed_data)

    # вызываем группу задач
    run_etl()

# создаем DAG
etl_dag = modern_etl_pipeline()