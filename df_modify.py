from read_df_from_postgres import df
import pandas as pd
import numpy as np
import time
from pandasql import sqldf
from sql_queries import query_df_exited_age_correlation, query_df_exited_salary_correlation


df.drop('rownumber', axis=1, inplace=True)

index_to_be_null = np.random.randint(10000, size=30)

df.loc[index_to_be_null, ['balance','creditscore','geography']] = np.nan

most_occured_country = df['geography'].value_counts().index[0]
df['geography'].fillna(value=most_occured_country, inplace=True)

avg_balance = df['balance'].mean()
df['balance'].fillna(value=avg_balance, inplace=True)

median_creditscore = df['creditscore'].median()
df['creditscore'].fillna(value=median_creditscore, inplace=True)


df_creditscore = df[['geography', 'gender', 'exited', 'creditscore']].groupby(['geography','gender']).agg({'creditscore':'mean', 'exited':'sum'})
df_creditscore.rename(columns={'exited':'total # exited', 'creditscore':'avg of credit score'}, inplace=True)
df_creditscore.reset_index(inplace=True)


df_creditscore.sort_values('avg of credit score', inplace=True)

df_exited_age_correlation = sqldf(query_df_exited_age_correlation)


df_salary = df[['geography','gender','exited','estimatedsalary']].groupby(['geography','gender']).agg({'estimatedsalary':'mean'}).sort_values('estimatedsalary')
df_salary.reset_index(inplace=True)

min_salary = round(df_salary['estimatedsalary'].min(),0)

df['is_greater'] = df['estimatedsalary'].apply(lambda x: 1 if x>min_salary else 0)

df_exited_salary_correlation = sqldf(query_df_exited_salary_correlation)

df_exited_salary_correlation['correlation'].value_counts()
