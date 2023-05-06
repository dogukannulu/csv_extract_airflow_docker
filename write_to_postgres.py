import psycopg2
import os
import logging
import pandas as pd

postgres_host = os.environ.get('postgres_host')
postgres_database = os.environ.get('postgres_database')
postgres_user = os.environ.get('postgres_user')
postgres_password = int(os.environ.get('postgres_password'))
postgres_port = int(os.environ.get('postgres_port'))

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

# Connect to the PostgreSQL server

conn = psycopg2.connect(
    host=postgres_host,
    database=postgres_database,
    user=postgres_user,
    password=postgres_password,
    port=postgres_port
)

# Create a cursor object to execute SQL queries
cur = conn.cursor()

# Create the table if it doesn't exist
cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_creditscore (Geography VARCHAR(50), Gender VARCHAR(20), avg of credit score FLOAT, total # exited INTEGER)""")

# Load the CSV data into the table
with open('churn_modelling.csv', 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    next(csvreader) # skip header row
    for row in csvreader:
        cur.execute("""INSERT INTO churn_modelling (RowNumber, CustomerId, Surname, CreditScore, Geography, Gender, Age, 
        Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)""", 
        (int(row[0]), int(row[1]), str(row[2]), int(row[3]), str(row[4]), str(row[5]), int(row[6]), int(row[7]), float(row[8]), int(row[9]), int(row[10]), int(row[11]), float(row[12]), int(row[13])))

# Commit the changes and close the cursor and connection
conn.commit()
cur.close()
conn.close()