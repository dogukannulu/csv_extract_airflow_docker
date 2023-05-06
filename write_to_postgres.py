import psycopg2
import os
import logging
import pandas as pd
import csv

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
with open('df_creditscore.csv', 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    next(csvreader) # skip header row
    for row in csvreader:
        cur.execute("""INSERT INTO churn_modelling_creditscore (Geography, Gender, avg of credit score, total # exited) VALUES (%s, %s, %s, %s)""", 
        (row[0], row[1], float(row[2]), int(row[3])))

# Create the table if it doesn't exist
cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_age_correlation (Geography VARCHAR(50), Gender VARCHAR(20), exited INTEGER, avg_age FLOAT, avg_salary FLOAT,number_of_exited_or_not INTEGER)""")

# Load the CSV data into the table
with open('df_exited_age_correlation.csv', 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    next(csvreader) # skip header row
    for row in csvreader:
        cur.execute("""INSERT INTO churn_modelling_exited_age_correlation (Geography, Gender, exited, avg_age, avg_salary) VALUES (%s, %s, %s, %s, %s)""", 
        (row[0], row[1], float(row[2]), float(row[3]), int(row[4])))

# Create the table if it doesn't exist
cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_salary_correlation  (exited INTEGER, is_greater INTEGER, correlation INTEGER)""")

# Load the CSV data into the table
with open('df_exited_salary_correlation.csv', 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    next(csvreader) # skip header row
    for row in csvreader:
        cur.execute("""INSERT INTO churn_modelling_creditscore (exited, is_greater, correlation) VALUES (%s, %s, %s, %s)""", 
        (int(row[0]), int(row[1]), int(row[2])))

# Commit the changes and close the cursor and connection
conn.commit()
cur.close()
conn.close()