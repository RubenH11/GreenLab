import sys
import pandas as pd
import time

if __name__ == "__main__":

    csv_size = sys.argv[1]

    try:
        start = time.time()
        # df = pd.read_csv('../data/' + str(csv_size) + '.csv')
        df = pd.read_parquet('../df_'+str(csv_size)+'_parquet.parquet')
        mdf_col_loan_amount = df['loan_amnt']

        for i in range(1):
            mdf_col_loan_amount.mean()
        duration = time.time() - start
        print('took ' + str(duration) + ' seconds')
        print('mean ' + str(mdf_col_loan_amount.mean()))

    except ValueError as e:
        print("error " + str(e))