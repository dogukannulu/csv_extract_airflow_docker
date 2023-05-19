from read_df_from_postgres import df
import pandas as pd
import numpy as np
from pandasql import sqldf
from pathlib import Path
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
df_creditscore.rename(columns={'exited':'total_exited', 'creditscore':'avg_credit_score'}, inplace=True)
df_creditscore.reset_index(inplace=True)


df_creditscore.sort_values('avg_credit_score', inplace=True)

df_exited_age_correlation = sqldf(query_df_exited_age_correlation)


df_salary = df[['geography','gender','exited','estimatedsalary']].groupby(['geography','gender']).agg({'estimatedsalary':'mean'}).sort_values('estimatedsalary')
df_salary.reset_index(inplace=True)

min_salary = round(df_salary['estimatedsalary'].min(),0)

df['is_greater'] = df['estimatedsalary'].apply(lambda x: 1 if x>min_salary else 0)

df_exited_salary_correlation = sqldf(query_df_exited_salary_correlation)

df_exited_salary_correlation['correlation'].value_counts()
print(df_creditscore)
print(df_exited_age_correlation)
print(df_exited_salary_correlation)

# df_creditscore_path = Path(r"/Users/dogukanulu/Desktop/codebase/csv_extract_airflow_docker/df_creditscore.csv")
# df_exited_age_correlation_path = Path(r"/Users/dogukanulu/Desktop/codebase/csv_extract_airflow_docker/df_exited_age_correlation.csv")
# df_exited_salary_correlation_path = Path(r"/Users/dogukanulu/Desktop/codebase/csv_extract_airflow_docker/df_exited_salary_correlation.csv")

# if (not df_creditscore_path.is_file()) and (not df_exited_age_correlation_path.is_file()) and (not df_exited_salary_correlation_path.is_file()):
#     df_creditscore.to_csv('df_creditscore.csv',header=True)
#     df_exited_age_correlation.to_csv('df_exited_age_correlation.csv',header=True)
#     df_exited_salary_correlation.to_csv('df_exited_salary_correlation.csv',header=True)
