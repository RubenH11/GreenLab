import sys
import modin.pandas as pd
import time

if __name__ == "__main__":

    csv_size = sys.argv[1]

    try:
        start = time.time()
        # df = pd.read_csv('../data/' + str(csv_size) + '.csv') 
        df = pd.read_parquet('../df_'+str(csv_size)+'_parquet.parquet')
        for i in range(25):
            df.isna()
        duration = time.time() - start
        print('took ' + str(duration) + ' seconds')
        
    except ValueError as e:
        print("error " + str(e))