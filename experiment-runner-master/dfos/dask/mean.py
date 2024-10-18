import sys
import dask.dataframe as dd
import time

if __name__ == "__main__":

    csv_size = sys.argv[1]

    try:
        start = time.time()
        # df = dd.read_csv('../data/' + str(csv_size) + '.csv')
        df = dd.read_parquet('../df_'+str(csv_size)+'_parquet.parquet')
        mdf_col_loan_amount = df['loan_amnt'].compute()

        for i in range(1):
            mdf_col_loan_amount.mean()
        duration = time.time() - start
        print('took ' + str(duration) + ' seconds')
        print('mean ' + str(mdf_col_loan_amount.mean()))

    except ValueError as e:
        print("error " + str(e))