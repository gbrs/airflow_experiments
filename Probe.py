from datetime import datetime, timedelta

file_path = "J:/docker/airflow/data/output/time_log.txt"

with open(file_path, 'a') as file:
    # file.write(datetime.now().strftime("%H:%M:%S.%f")[:-3])
    print(datetime.now().strftime("%H:%M:%S.%f")[:-3])
