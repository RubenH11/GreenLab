
from daskDfos import DaskDFOs
from polarsDfos import PolarsDFOs
from pandasDfos import PandasDFOs
import pandas as pd
import dask.dataframe as dd
import numpy as np
import polars as pl

# df = pl.DataFrame({
#     'A': [1, 20, '3', 4, 1],
#     'B': [10, 20, 40, 20, '50'],
#     'C': [100, 200, np.nan, 20, 500]
# }, strict=False)
df = pd.read_csv('../data/large.csv', low_memory=False, nrows=500)
df.to_csv('small.csv', index=False)

# df.head(50).to_csv('loan2.csv', index=False)
# df = df.fill_nan(None)
# ddf = dd.from_pandas(df, npartitions=2)

# df = df.with_columns([
#     df[col].cast(pl.Float64) for col in df.columns if df[col].dtype == 
# # ])
# df = df.fill_null(np.nan)

# plp = PolarsDFOs(df['mths_since_last_delinq', 'mths_since_last_record', 'open_acc'])
# print(plp.dataset, '\n')


# print(plp.isna())
# print(plp.fillna())
# print(plp.concat(df.select(pl.col('A').alias('D'))))
# print(plp.merge(df[:,0:2], df[:,0:3].drop('B'), on='A'))
# print(pp.groupby(df.iloc[:,0]))

# merge
# cols1 = (df.iloc[:,0:2])
# cols2 = (df.iloc[:,0:3].drop(columns=df.columns[1]))
# on = df.columns[0]
# print(cols1, '\n', cols2)
# print(p.merge(cols1, cols2, on=on))
