from airflow import DAG
import sys
from datetime import datetime, timedelta

sys.path.append('../')

#import write_csv_to_postgres,read_df_from_postgres,write_df_to_postgres,df_modify
import read_df_from_postgres


from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

start_date = datetime(2023, 1, 1, 12, 10)

default_args = {
    'owner': 'dogukanulu',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('csv_extract_airflow_docker', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    download_data = BashOperator(task_id='download_data',
                                 bash_command='curl -O /Users/dogukanulu/Desktop/codebase/csv_extract_airflow_docker/churn_modelling.csv https://raw.githubusercontent.com/dogukannulu/datasets/master/Churn_Modelling.csv',
                                 retries=1, execution_timeout=timedelta(minutes=10))

    
    #create_postgres_table_task = PythonOperator(task_id='create_postgres_table', pyhton_callable=create_postgres_table)
    
    # write_csv_to_postgres_task = PythonOperator(task_id='write_csv_to_postgres', python_callable=write_csv_to_postgres)

    # read_df_from_postgres_task = PythonOperator(task_id='read_df_from_postgres', python_callable=read_df_from_postgres)

    # df_modify_task = PythonOperator(task_id='df_modify', python_callable=df_modify)

    # write_df_to_postgres_task = PythonOperator(task_id='write_df_to_postgres', python_callable=write_df_to_postgres)

    # download_data >> write_csv_to_postgres_task >> read_df_from_postgres_task >> df_modify_task >> write_df_to_postgres_task

    download_data 