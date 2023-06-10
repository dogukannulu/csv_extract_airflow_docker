"""
Downloads the csv file from the URL. Creates a new table in the Postgres server.
Reads the file as a dataframe and inserts each record to the Postgres table. 
"""
import psycopg2
import os
import traceback
import logging
import pandas as pd
import urllib.request

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')


postgres_host = os.environ.get('postgres_host')
postgres_database = os.environ.get('postgres_database')
postgres_user = os.environ.get('postgres_user')
postgres_password = os.environ.get('postgres_password')
postgres_port = os.environ.get('postgres_port')
dest_folder = os.environ.get('dest_folder')

url = "https://raw.githubusercontent.com/dogukannulu/datasets/master/Churn_Modelling.csv"
destination_path = f'{dest_folder}/churn_modelling.csv' 

try:
    conn = psycopg2.connect(
        host=postgres_host,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password,
        port=postgres_port
    )
    cur = conn.cursor()
    logging.info('Postgres server connection is successful')
except Exception as e:
    traceback.print_exc()
    logging.error("Couldn't create the Postgres connection")


def download_file_from_url(url: str, dest_folder: str):
    """
    Download a file from a specific URL and download to the local direcory
    """
    if not os.path.exists(str(dest_folder)):
        os.makedirs(str(dest_folder))  # create folder if it does not exist

    try:
        urllib.request.urlretrieve(url, destination_path)
        logging.info('csv file downloaded successfully to the working directory')
    except Exception as e:
        logging.error(f'Error while downloading the csv file due to: {e}')
        traceback.print_exc()


def create_postgres_table():
    """
    Create the Postgres table with a desired schema
    """
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling (RowNumber INTEGER PRIMARY KEY, CustomerId INTEGER, 
        Surname VARCHAR(50), CreditScore INTEGER, Geography VARCHAR(50), Gender VARCHAR(20), Age INTEGER, 
        Tenure INTEGER, Balance FLOAT, NumOfProducts INTEGER, HasCrCard INTEGER, IsActiveMember INTEGER, EstimatedSalary FLOAT, Exited INTEGER)""")
        
        logging.info(' New table churn_modelling created successfully to postgres server')
    except:
        logging.warning(' Check if the table churn_modelling exists')


def write_to_postgres():
    """
    Create the dataframe and write to Postgres table if it doesn't already exist
    """
    df = pd.read_csv(f'{dest_folder}/churn_modelling.csv')
    inserted_row_count = 0

    for _, row in df.iterrows():
        count_query = f"""SELECT COUNT(*) FROM churn_modelling WHERE RowNumber = {row['RowNumber']}"""
        cur.execute(count_query)
        result = cur.fetchone()
        
        if result[0] == 0:
            inserted_row_count += 1
            cur.execute("""INSERT INTO churn_modelling (RowNumber, CustomerId, Surname, CreditScore, Geography, Gender, Age, 
            Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)""", 
            (int(row[0]), int(row[1]), str(row[2]), int(row[3]), str(row[4]), str(row[5]), int(row[6]), int(row[7]), float(row[8]), int(row[9]), int(row[10]), int(row[11]), float(row[12]), int(row[13])))

    logging.info(f' {inserted_row_count} rows from csv file inserted into churn_modelling table successfully')

def write_csv_to_postgres_main():
    download_file_from_url(url, dest_folder)
    create_postgres_table()
    write_to_postgres()
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    download_file_from_url(url, dest_folder)
    create_postgres_table()
    write_to_postgres()
    conn.commit()
    cur.close()
    conn.close()
