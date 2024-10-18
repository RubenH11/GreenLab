import sys
import pandas as pd
import time

if __name__ == "__main__":

    csv_size = sys.argv[1]

    try:
        start = time.time()
        df = pd.read_csv('dfos/data/' + str(csv_size) + '.csv')
        for i in range(1):
            df.isna()
        duration = time.time() - start
        print('took ' + str(duration) + ' seconds')
        
    except ValueError as e:
        print("error " + str(e))