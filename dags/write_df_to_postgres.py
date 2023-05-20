import os
import logging
import psycopg2
import traceback
from create_df_and_modify import create_base_df, create_creditscore_df, create_exited_age_correlation, create_exited_salary_correlation

postgres_host = os.environ.get('postgres_host')
postgres_database = os.environ.get('postgres_database')
postgres_user = os.environ.get('postgres_user')
postgres_password = os.environ.get('postgres_password')
postgres_port = os.environ.get('postgres_port')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

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


def create_new_tables_in_postgres():
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_creditscore (geography VARCHAR(50), gender VARCHAR(20), avg_credit_score FLOAT, total_exited INTEGER)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_age_correlation (geography VARCHAR(50), gender VARCHAR(20), exited INTEGER, avg_age FLOAT, avg_salary FLOAT,number_of_exited_or_not INTEGER)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_salary_correlation  (exited INTEGER, is_greater INTEGER, correlation INTEGER)""")
        logging.info("3 tables created successfully in Postgres server")
    except Exception as e:
        traceback.print_exc()
        logging.error(f'Tables cannot be created due to: {e}')


def insert_creditscore_table(df_creditscore):
    query = "INSERT INTO churn_modelling_creditscore (geography, gender, avg_credit_score, total_exited) VALUES (%s,%s,%s,%s)"
    row_count = 0
    for _, row in df_creditscore.iterrows():
        values = (row['geography'],row['gender'],row['avg_credit_score'],row['total_exited'])
        cur.execute(query,values)
        row_count += 1
    
    logging.info(f"{row_count} rows inserted into table churn_modelling_creditscore")


def insert_exited_age_correlation_table(df_exited_age_correlation):
    query = """INSERT INTO churn_modelling_exited_age_correlation (Geography, Gender, exited, avg_age, avg_salary, number_of_exited_or_not) VALUES (%s,%s,%s,%s,%s,%s)"""
    row_count = 0
    for _, row in df_exited_age_correlation.iterrows():
        values = (row['geography'],row['gender'],row['exited'],row['avg_age'],row['avg_salary'],row['number_of_exited_or_not'])
        cur.execute(query,values)
        row_count += 1
    
    logging.info(f"{row_count} rows inserted into table churn_modelling_exited_age_correlation")


def insert_exited_salary_correlation_table(df_exited_salary_correlation):
    query = """INSERT INTO churn_modelling_exited_salary_correlation (exited, is_greater, correlation) VALUES (%s,%s,%s)"""
    row_count = 0
    for _, row in df_exited_salary_correlation.iterrows():
        values = (int(row['exited']),int(row['is_greater']),int(row['correlation']))
        cur.execute(query,values)
        row_count += 1

    logging.info(f"{row_count} rows inserted into table churn_modelling_exited_salary_correlation")


def write_df_to_postgres_main():
    main_df = create_base_df(cur)
    df_creditscore = create_creditscore_df(main_df)
    df_exited_age_correlation = create_exited_age_correlation(main_df)
    df_exited_salary_correlation = create_exited_salary_correlation(main_df)

    create_new_tables_in_postgres()
    insert_creditscore_table(df_creditscore)
    insert_exited_age_correlation_table(df_exited_age_correlation)
    insert_exited_salary_correlation_table(df_exited_salary_correlation)

    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    main_df = create_base_df(cur)
    df_creditscore = create_creditscore_df(main_df)
    df_exited_age_correlation = create_exited_age_correlation(main_df)
    df_exited_salary_correlation = create_exited_salary_correlation(main_df)

    create_new_tables_in_postgres()
    insert_creditscore_table(df_creditscore)
    insert_exited_age_correlation_table(df_exited_age_correlation)
    insert_exited_salary_correlation_table(df_exited_salary_correlation)

    conn.commit()
    cur.close()
    conn.close()