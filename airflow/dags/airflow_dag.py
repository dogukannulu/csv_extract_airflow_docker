import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

parent_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_folder)

from write_csv_to_postgres import write_csv_to_postgres_main
from write_df_to_postgres import write_df_to_postgres_main

start_date = datetime(2023, 1, 1, 12, 10)

default_args = {
    'owner': 'dogukanulu',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('csv_extract_airflow_docker', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    write_csv_to_postgres = PythonOperator(
        task_id='write_csv_to_postgres',
        python_callable=write_csv_to_postgres_main,
        retries=1,
        retry_delay=timedelta(seconds=15))

    write_df_to_postgres = PythonOperator(
        task_id='write_df_to_postgres',
        python_callable=write_df_to_postgres_main,
        retries=1,
        retry_delay=timedelta(seconds=15))
    
    write_csv_to_postgres >> write_df_to_postgres 