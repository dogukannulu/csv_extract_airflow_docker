import psycopg2
import os
import logging
import pandas as pd
from df_modify import df_creditscore, df_exited_age_correlation, df_exited_salary_correlation

postgres_host = os.environ.get('postgres_host')
postgres_database = os.environ.get('postgres_database')
postgres_user = os.environ.get('postgres_user')
postgres_password = int(os.environ.get('postgres_password'))
postgres_port = int(os.environ.get('postgres_port'))

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

# Connect to the PostgreSQL server
#engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')

conn = psycopg2.connect(
    host=postgres_host,
    database=postgres_database,
    user=postgres_user,
    password=postgres_password,
    port=postgres_port
)
cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_creditscore (geography VARCHAR(50), gender VARCHAR(20), avg_credit_score FLOAT, total_exited INTEGER)""")
cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_age_correlation (geography VARCHAR(50), gender VARCHAR(20), exited INTEGER, avg_age FLOAT, avg_salary FLOAT,number_of_exited_or_not INTEGER)""")
cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_salary_correlation  (exited INTEGER, is_greater INTEGER, correlation INTEGER)""")

#df_creditscore.to_sql('churn_modelling_creditscore', engine, index=False, if_exists='replace')


query_1 = "INSERT INTO churn_modelling_creditscore (geography, gender, avg_credit_score, total_exited) VALUES (%s,%s,%s,%s)"
# Load the CSV data into the table
for index, row in df_creditscore.iterrows():
    values_1 = (row['geography'],row['gender'],row['avg_credit_score'],row['total_exited'])
    cur.execute(query_1,values_1)

# Create the table if it doesn't exist

query_2 = """INSERT INTO churn_modelling_exited_age_correlation (Geography, Gender, exited, avg_age, avg_salary, number_of_exited_or_not) VALUES (%s,%s,%s,%s,%s,%s)"""
# Load the CSV data into the table
for index, row in df_exited_age_correlation.iterrows():
    values_2 = (row['geography'],row['gender'],row['exited'],row['avg_age'],row['avg_salary'],row['number_of_exited_or_not'])
    cur.execute(query_2,values_2)

# Create the table if it doesn't exist

query_3 = """INSERT INTO churn_modelling_exited_salary_correlation (exited, is_greater, correlation) VALUES (%s,%s,%s)"""
# Load the CSV data into the table
for index, row in df_exited_salary_correlation.iterrows():
    values_3 = (int(row['exited']),int(row['is_greater']),int(row['correlation']))
    cur.execute(query_3,values_3)

# Commit the changes and close the cursor and connection
conn.commit()
cur.close()
conn.close()