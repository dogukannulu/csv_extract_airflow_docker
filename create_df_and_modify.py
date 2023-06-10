"""
Creates a base dataframe out of churn_modelling table and creates 3 separate dataframes out of it.
"""
import os
import logging
import psycopg2
import traceback
import numpy as np
import pandas as pd

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


def create_base_df(cur):
    """
    Base dataframe of churn_modelling table
    """
    cur.execute("""SELECT * FROM churn_modelling""")
    rows = cur.fetchall()

    col_names = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=col_names)

    df.drop('rownumber', axis=1, inplace=True)
    index_to_be_null = np.random.randint(10000, size=30)
    df.loc[index_to_be_null, ['balance','creditscore','geography']] = np.nan
    
    most_occured_country = df['geography'].value_counts().index[0]
    df['geography'].fillna(value=most_occured_country, inplace=True)
    
    avg_balance = df['balance'].mean()
    df['balance'].fillna(value=avg_balance, inplace=True)

    median_creditscore = df['creditscore'].median()
    df['creditscore'].fillna(value=median_creditscore, inplace=True)

    return df


def create_creditscore_df(df):
    df_creditscore = df[['geography', 'gender', 'exited', 'creditscore']].groupby(['geography','gender']).agg({'creditscore':'mean', 'exited':'sum'})
    df_creditscore.rename(columns={'exited':'total_exited', 'creditscore':'avg_credit_score'}, inplace=True)
    df_creditscore.reset_index(inplace=True)

    df_creditscore.sort_values('avg_credit_score', inplace=True)

    return df_creditscore


def create_exited_age_correlation(df):
    df_exited_age_correlation = df.groupby(['geography', 'gender', 'exited']).agg({
    'age': 'mean',
    'estimatedsalary': 'mean',
    'exited': 'count'
    }).rename(columns={
        'age': 'avg_age',
        'estimatedsalary': 'avg_salary',
        'exited': 'number_of_exited_or_not'
    }).reset_index().sort_values('number_of_exited_or_not')

    return df_exited_age_correlation


def create_exited_salary_correlation(df):
    df_salary = df[['geography','gender','exited','estimatedsalary']].groupby(['geography','gender']).agg({'estimatedsalary':'mean'}).sort_values('estimatedsalary')
    df_salary.reset_index(inplace=True)

    min_salary = round(df_salary['estimatedsalary'].min(),0)

    df['is_greater'] = df['estimatedsalary'].apply(lambda x: 1 if x>min_salary else 0)

    df_exited_salary_correlation = pd.DataFrame({
    'exited': df['exited'],
    'is_greater': df['estimatedsalary'] > df['estimatedsalary'].min(),
    'correlation': np.where(df['exited'] == (df['estimatedsalary'] > df['estimatedsalary'].min()), 1, 0)
    })

    return df_exited_salary_correlation

