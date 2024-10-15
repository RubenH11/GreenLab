import pandas as pd

class PandasDFOs:
    def __init__(self, dataset):
        self.dataset: pd.DataFrame = dataset

    def isna(self):
        return self.dataset.isna()
    def replace(self, valueToReplaceWithApple):
        return self.dataset.replace(valueToReplaceWithApple, 'apple')
    def groupby(self, column):
        return self.dataset.groupby(column)
    def sort(self, column_name):
        return self.dataset.sort_values(by=column_name)
    def mean(self, column):
        return column.mean()
    def drop(self, column_name):
        return self.dataset.drop(columns=column_name)
    def dropna(self):
        return self.dataset.dropna()
    def fillna(self):
        return self.dataset.fillna('apple')
    def concat(self, column):
        return pd.concat([self.dataset, column], axis=1) # 1 = columns
    def merge(self, cols1, cols2, on):
        return cols1.merge(cols2, on=on)
    