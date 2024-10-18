import sys
import polars as pl
import time

if __name__ == "__main__":

    csv_size = sys.argv[1]

    try:
        start = time.time()
        # df = pl.read_csv('../data/' + str(csv_size) + '.csv')
        df = pl.read_parquet('../df_'+str(csv_size)+'_parquet.parquet')
        for i in range(1):
            df.fill_nan('apple')
        duration = time.time() - start
        print('took ' + str(duration) + ' seconds')
        
    except ValueError as e:
        print("error " + str(e))