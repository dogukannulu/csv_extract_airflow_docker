from read_df_from_postgres import df
import numpy as np
import time


df.drop('rownumber', axis=1, inplace=True)

print(df[['balance', 'geography']].head())

print(df.shape)

index_to_be_null = np.random.randint(10000, size=20)
print(index_to_be_null)

df.loc[index_to_be_null, ['Balance','Geography']] = np.nan

print(df.isna().sum())

time.sleep(10)