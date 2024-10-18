import sys
import pandas as pd
import time

if __name__ == "__main__":

    csv_size = sys.argv[1]

    try:
        start = time.time()
        # df = pd.read_csv('../data/' + str(csv_size) + '.csv')
        df = pd.read_parquet('../df_'+str(csv_size)+'_parquet.parquet')

        mdf_col_grade = df['grade']
        column = mdf_col_grade

        for i in range(25):
            pd.concat([df, column], axis=1) # 1 = columns
        duration = time.time() - start
        print('took ' + str(duration) + ' seconds')
        
    except ValueError as e:
        print("error " + str(e))