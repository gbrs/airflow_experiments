from datetime import datetime
from airflow.decorators import dag, task
import csv
import os


@dag(
    dag_id='modern_etl_docker',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def modern_etl_docker():
    # Пути внутри контейнера (если используете volume)
    BASE_DIR = "/opt/airflow/data"

    @task
    def extract():
        input_path = f"{BASE_DIR}/input/source_data.csv"
        print(f"Reading from: {input_path}")

        data = []
        with open(input_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append(row)

        return data

    @task
    def transform(data: list):
        transformed_data = []
        for row in data:
            new_row = row.copy()
            new_row['name_upper'] = row['name'].upper()
            new_row['bonus'] = str(float(row['salary']) * 0.1)
            transformed_data.append(new_row)

        return transformed_data

    @task
    def load(data: list):
        output_path = f"{BASE_DIR}/output/processed_data.csv"

        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        if data:
            fieldnames = list(data[0].keys())
            with open(output_path, 'w', encoding='utf-8', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)

        return output_path

    # Поток выполнения
    load(transform(extract()))


# Создаем DAG
etl_docker_dag = modern_etl_docker()
