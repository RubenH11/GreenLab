import sys
import dask.dataframe as dd
import time

if __name__ == "__main__":

    csv_size = sys.argv[1]
    column_name = 'loan_amnt'

    try:
        start = time.time()
        # df = dd.read_csv('../data/' + str(csv_size) + '.csv')
        df = dd.read_parquet('../df_'+str(csv_size)+'_parquet.parquet')
        for i in range(1):
            df.sort_values(by=column_name)
        duration = time.time() - start
        print('took ' + str(duration) + ' seconds')
        
    except ValueError as e:
        print("error " + str(e))