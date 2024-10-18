import pandas as pd
import pickle
import parquet


# df = pd.read_csv('dfos/data/large.csv', low_memory=False)


# with open('dataframe.pkl', 'wb') as f:
#     pickle.dump(df,f)


df = pd.read_csv('../data/large.csv', low_memory=False)
df.to_parquet('df_large_parquet.parquet')