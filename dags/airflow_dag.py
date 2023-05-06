from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

start_date = datetime(2023, 1, 1, 12, 10)

default_args = {
    'owner': 'dogukanulu',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('csv_extract_airflow_docker', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    download_data = BashOperator(task_id='download_data',
                                 bash_command='''wget -O /Users/dogukanulu/codebase/csv_extract_airflow_docker/churn_modelling.csv.zip 
                                 https://raw.githubusercontent.com/dogukannulu/datasets/master/Churn_Modelling.csv''',
                                 retries=1, retry_delay=timedelta(seconds=15))

    download_data