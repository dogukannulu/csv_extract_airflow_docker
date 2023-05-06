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
cur.execute("""SELECT * FROM churn_modelling""")

rows = cur.fetchall()

# Get the column names from the cursor description
col_names = [desc[0] for desc in cur.description]

# Convert the list of tuples into a Pandas DataFrame
df = pd.DataFrame(rows, columns=col_names)
print(df.head())

cur.close()
conn.close()
