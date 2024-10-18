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
            df.select(pl.col(pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8).is_nan())
        duration = time.time() - start
        print('took ' + str(duration) + ' seconds')
        
    except ValueError as e:
        print("error " + str(e))