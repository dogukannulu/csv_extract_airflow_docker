import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dest_folder = os.environ.get('dest_folder')

start_date = datetime(2023, 1, 1, 12, 10)

default_args = {
    'owner': 'dogukanulu',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('csv_extract_airflow_docker', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    write_csv_to_postgres = BashOperator(
        task_id='write_csv_to_postgres', 
        bash_command=f'python3 {dest_folder}/write_csv_to_postgres.py',
        retries=1, retry_delay=timedelta(seconds=15))

    write_df_to_postgres = BashOperator(
        task_id='write_df_to_postgres', 
        bash_command=f'python3 {dest_folder}/write_df_to_postgres.py',
        retries=1, retry_delay=timedelta(seconds=15))
    
    write_csv_to_postgres >> write_df_to_postgres 