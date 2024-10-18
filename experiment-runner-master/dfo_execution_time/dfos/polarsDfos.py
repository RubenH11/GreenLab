import polars as pl

class PolarsDFOs:
    def __init__(self, dataset):
        self.dataset: pl.DataFrame = dataset

    def isna(self):
        return self.dataset.select(pl.col(pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8).is_nan())

    def replace(self, valueToReplaceWithApple):
        return self.dataset.with_columns(
            [pl.col(c).replace(valueToReplaceWithApple, 'apple') for c in self.dataset.select(pl.col(pl.String)).columns]
        )

    def groupby(self, column):
        return self.dataset.group_by(column)

    def sort(self, column_name):
        return self.dataset.sort(column_name)

    def mean(self, column_name):
        return self.dataset.select(pl.col(column_name).mean()).item()

    def drop(self, column_name):
        return self.dataset.drop(column_name)

    def dropna(self):
        return self.dataset.drop_nulls()

    def fillna(self):
        return self.dataset.fill_nan('apple')

    def concat(self, column: pl.DataFrame):
        return pl.concat([self.dataset, column], how='horizontal')

    def merge(self, cols1 : pl.DataFrame, cols2 : pl.DataFrame, on):
        return cols1.join(cols2, on=on, how='inner')