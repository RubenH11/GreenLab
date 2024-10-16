
from daskDfos import DaskDFOs
from polarsDfos import PolarsDFOs
from pandasDfos import PandasDFOs
from modinDfos import ModinDFOs
import pandas as pd
import dask.dataframe as dd
import numpy as np
import polars as pl
import modin.pandas as mpd
import time
import math

# df = pd.DataFrame({
#     'term': ['Alice', 'OWN', 'Charlie', 'David', 'Eva'],
#     'loan_amnt': [25, 30, 35, 40, 22],
#     'int_rate': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'],
#     'installment': ['New York2', 'Los Angeles2', 'Chicago2', 'Houston2', 'Phoenix2'],
#     'grade': [70000, 80000, np.nan, 100000, 60000]
# })

# df = pd.read_csv('../../data/small.csv', low_memory=False, nrows=500)
# pdf = pl.from_dataframe(df)
# print('done reading')
# df.to_csv('small.csv', index=False)

# df.head(50).to_csv('loan2.csv', index=False)
# df = df.fill_nan(None)
# ddf = dd.from_pandas(df, npartitions=2)

# pdfos = PolarsDFOs(pdf)

# print(pdfos.)

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


def set_functions_for_dataset():
        print('start reading small.csv')
        df = pd.read_csv('../../data/large.csv', low_memory=False, nrows=1000000)

        print('start processing to dataframes')

        pandasDfos = PandasDFOs(df)

        mdf = mpd.DataFrame(df)
        modinDfos = ModinDFOs(mdf)

        pdf = pl.from_pandas(df)
        pdf.fill_null(np.nan) # required for isna and fillna
        polarsDfos = PolarsDFOs(pdf)

        ddf = dd.from_pandas(df, npartitions=2)
        daskDfos = DaskDFOs(ddf)

        df_col_grade = df['grade']
        mdf_col_grade = mdf['grade']
        pdf_col_grade = pdf['grade'].to_frame()
        pdf_col_grade_as_renamed = pdf['grade'].alias('renamed').to_frame()
        ddf_col_grade = ddf['grade'].compute()

        df_col_loan_amount = df['loan_amnt']
        mdf_col_loan_amount = mdf['loan_amnt']
        pdf_col_loan_amount = pdf['loan_amnt']
        ddf_col_loan_amount = ddf['loan_amnt'].compute()

        df_cols_term__int_rate = df[['term', 'int_rate']]
        df_cols_term__installment = df[['term', 'installment']]
        mdf_cols_term__int_rate = mdf[['term', 'int_rate']]
        mdf_cols_term__installment = mdf[['term', 'installment']]
        pdf_cols_term__int_rate = pdf['term', 'int_rate']
        pdf_cols_term__installment = pdf['term', 'installment']
        ddf_cols_term__int_rate = ddf[['term', 'int_rate']].compute()
        ddf_cols_term__installment = ddf[['term', 'installment']].compute()

        print('defining functions')
        return {
            "Pandas": {
                "isna": pandasDfos.isna,
                "replace": lambda : pandasDfos.replace('OWN'),
                "groupby": lambda : pandasDfos.groupby(df_col_grade),
                "sort": lambda : pandasDfos.sort('loan_amnt'),
                "mean": lambda : pandasDfos.mean(df_col_loan_amount),
                "drop": lambda : pandasDfos.drop('loan_amnt'),
                "dropna": pandasDfos.dropna,
                "fillna": pandasDfos.fillna,
                "concat": lambda : pandasDfos.concat(df_col_grade),
                # "merge": lambda : pandasDfos.merge(df_cols_term__int_rate, df_cols_term__installment, 'term'),
            },
            "Modin": {
               "isna": modinDfos.isna,
                "replace": lambda : modinDfos.replace('OWN'),
                "groupby": lambda : modinDfos.groupby(mdf_col_grade),
                "sort": lambda : modinDfos.sort('loan_amnt'),
                "mean": lambda : modinDfos.mean(mdf_col_loan_amount),
                "drop": lambda : modinDfos.drop('loan_amnt'),
                "dropna": modinDfos.dropna,
                "fillna": modinDfos.fillna,
                "concat": lambda : modinDfos.concat(mdf_col_grade),
                # "merge": lambda : modinDfos.merge(mdf_cols_term__int_rate, mdf_cols_term__installment, 'term'),
            },
            "Polars": {
                "isna": polarsDfos.isna,
                "replace": lambda : polarsDfos.replace('OWN'),
                "groupby": lambda : polarsDfos.groupby(pdf_col_grade),
                "sort": lambda : polarsDfos.sort('loan_amnt'),
                "mean": lambda : polarsDfos.mean('loan_amnt'),
                "drop": lambda : polarsDfos.drop('loan_amnt'),
                "dropna": polarsDfos.dropna,
                "fillna": polarsDfos.fillna,
                "concat": lambda : polarsDfos.concat(pdf_col_grade_as_renamed),
                # "merge": lambda : polarsDfos.merge(pdf_cols_term__int_rate, pdf_cols_term__installment, 'term'),
            },
            "Dask": {
                "isna": daskDfos.isna,
                "replace": lambda : daskDfos.replace('OWN'),
                "groupby": lambda : daskDfos.groupby(ddf_col_grade),
                "sort": lambda : daskDfos.sort('loan_amnt'),
                "mean": lambda : daskDfos.mean(ddf_col_loan_amount),
                "drop": lambda : daskDfos.drop('loan_amnt'),
                "dropna": daskDfos.dropna,
                "fillna": daskDfos.fillna,
                "concat": lambda : daskDfos.concat(ddf_col_grade),
                # "merge": lambda : daskDfos.merge(ddf_cols_term__int_rate, ddf_cols_term__installment, 'term'),
            }
        }

functions = set_functions_for_dataset()

for lib in functions:
    for op in functions[lib]:
        print('executing: ' + lib + ' with ' + op)
        try:
            st = time.time()
            functions[lib][op]()
            print('success in ' + str(round(time.time()-st, 2)) + ' seconds')
        except Exception as e:
            print('error: ' + str(e))

