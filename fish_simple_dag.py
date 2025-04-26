from airflow import DAG
from airflow.decorators import task

from datetime import datetime


# описываем DAG
@dag(
    dag_id='simple_etl',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def simple_etl():
    # описываем таски

    @task
    def extract():
        import pandas as pd
        df = pd.read_csv('/path/to/input_data.csv')
        return df

    @task
    def transform(df):
        df['name'] = df['name'].str.upper()
        df['total'] = df['quantity'] * df['price']
        return df

    @task
    def load(df):
        import sqlite3
        conn = sqlite3.connect('/path/to/database.db')
        df.to_sql('processed_data', conn, if_exists='append', index=False)
        conn.close()

    # Поток выполнения
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

# Создаем DAG
simple_etl_dag = simple_etl()