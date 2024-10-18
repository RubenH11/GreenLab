import sys
import polars as pl
import time

if __name__ == "__main__":

    csv_size = sys.argv[1]

    try:
        start = time.time()
        # df = pl.read_csv('../data/' + str(csv_size) + '.csv')
        df = pl.read_parquet('../df_'+str(csv_size)+'_parquet.parquet')
        pdf_col_grade_as_renamed = df['grade'].alias('renamed').to_frame()

        column = pdf_col_grade_as_renamed

        for i in range(25):
            pl.concat([df, column], how='horizontal')
        duration = time.time() - start
        print('took ' + str(duration) + ' seconds')
        
    except ValueError as e:
        print("error " + str(e))