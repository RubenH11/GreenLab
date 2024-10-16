from EventManager.Models.RunnerEvents import RunnerEvents
from EventManager.EventSubscriptionController import EventSubscriptionController
from ConfigValidator.Config.Models.RunTableModel import RunTableModel
from ConfigValidator.Config.Models.FactorModel import FactorModel
from ConfigValidator.Config.Models.RunnerContext import RunnerContext
from ConfigValidator.Config.Models.OperationType import OperationType
from ExtendedTyping.Typing import SupportsStr
from ProgressManager.Output.OutputProcedure import OutputProcedure as output

from typing import Dict, List, Any, Optional
from pathlib import Path
from os.path import dirname, realpath

import time
import math
import numpy as np
import os
import paramiko

import pandas as pd
import modin.pandas as mpd
import dask.dataframe as dd
import polars as pl

# from dfos.pandasDfos import PandasDFOs
# from dfos.modinDfos import ModinDFOs
# from dfos.polarsDfos import PolarsDFOs
# from dfos.daskDfos import DaskDFOs

class PolarsDFOs:
    def __init__(self, dataset):
        self.dataset: pl.DataFrame = dataset

    def isna(self):
        return self.dataset.select(pl.all().is_nan())

    def replace(self, valueToReplaceWithApple):
        return self.dataset.with_columns(
            [pl.col(c).replace(valueToReplaceWithApple, 'apple') for c in self.dataset.columns]
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

    def merge(self, cols1 : pl.DataFrame, cols2, on):
        return cols1.join(cols2, on=on, how='inner')

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

class ModinDFOs:
    def __init__(self, dataset):
        self.dataset: mpd.DataFrame = dataset

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
        return mpd.concat([self.dataset, column], axis=1) # 1 = columns
    def merge(self, cols1, cols2, on):
        return cols1.merge(cols2, on=on)

class DaskDFOs:
    def __init__(self, dataset):
        self.dataset: dd.DataFrame = dataset

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
        return dd.concat([self.dataset, column], axis=1) # 1 = columns
    def merge(self, cols1, cols2, on):
        return cols1.merge(cols2, on=on)

class RunnerConfig:
    ROOT_DIR = Path(dirname(realpath(__file__)))

    # ================================ USER SPECIFIC CONFIG ================================
    """The name of the experiment."""
    name:                       str             = "pandas_versus_hpc"

    """The path in which Experiment Runner will create a folder with the name `self.name`, in order to store the
    results from this experiment. (Path does not need to exist - it will be created if necessary.)
    Output path defaults to the config file's path, inside the folder 'experiments'"""
    results_output_path:        Path            = ROOT_DIR / 'experiments'

    """Experiment operation type. Unless you manually want to initiate each run, use `OperationType.AUTO`."""
    operation_type:             OperationType   = OperationType.AUTO

    """The time Experiment Runner will wait after a run completes.
    This can be essential to accommodate for cooldown periods on some systems."""
    time_between_runs_in_ms:    int             = 600

    start_time = 0

    functions = {}

    # Dynamic configurations can be one-time satisfied here before the program takes the config as-is
    # e.g. Setting some variable based on some criteria
    def __init__(self):
        """Executes immediately after program start, on config load"""
        output.console_log(os.getcwd())

        EventSubscriptionController.subscribe_to_multiple_events([
            (RunnerEvents.BEFORE_EXPERIMENT, self.before_experiment),
            (RunnerEvents.BEFORE_RUN       , self.before_run       ),
            (RunnerEvents.START_RUN        , self.start_run        ),
            (RunnerEvents.START_MEASUREMENT, self.start_measurement),
            (RunnerEvents.INTERACT         , self.interact         ),
            (RunnerEvents.STOP_MEASUREMENT , self.stop_measurement ),
            (RunnerEvents.STOP_RUN         , self.stop_run         ),
            (RunnerEvents.POPULATE_RUN_DATA, self.populate_run_data),
            (RunnerEvents.AFTER_EXPERIMENT , self.after_experiment )
        ])
        self.run_table_model = None  # Initialized later
        # self.set_functions_for_dataset()
        output.console_log("Custom config loaded")

    def set_functions_for_dataset(self):
        output.console_log('start reading small.csv')
        df = pd.read_csv('data/small.csv', low_memory=False)

        output.console_log('start processing to dataframes')

        pandasDfos = PandasDFOs(df)

        # mdf = mpd.DataFrame(df)
        # modinDfos = ModinDFOs(mdf)

        pdf = pl.from_pandas(df)
        pdf.fill_null(np.nan) # required for isna and fillna
        polarsDfos = PolarsDFOs(pdf)

        ddf = dd.from_pandas(df, npartitions=2)
        daskDfos = DaskDFOs(ddf)

        df_col_grade = df['grade']
        # mdf_col_grade = mdf['grade']
        pdf_col_grade = pdf['grade']
        pdf_col_grade_as_renamed = pdf_col_grade.alias('renamed')
        ddf_col_grade = ddf['grade'].compute()

        df_col_loan_amount = df['loan_amnt']
        # mdf_col_loan_amount = mdf['loan_amnt']
        pdf_col_loan_amount = pdf['loan_amnt']
        ddf_col_loan_amount = ddf['loan_amnt'].compute()

        df_cols_term__int_rate = df[['term', 'int_rate']]
        df_cols_term__installment = df[['term', 'installment']]
        # mdf_cols_term__int_rate = mdf[['term', 'int_rate']]
        # mdf_cols_term__installment = mdf[['term', 'installment']]
        pdf_cols_term__int_rate = pdf.select([['term', 'int_rate']])
        pdf_cols_term__installment = pdf.select([['term', 'installment']])
        ddf_cols_term__int_rate = ddf[['term', 'int_rate']].compute()
        ddf_cols_term__installment = ddf[['term', 'installment']].compute()

        output.console_log('defining functions')
        self.functions = {
            "Pandas": {
                "isna": pandasDfos.isna,
                "replace": lambda : pandasDfos.replace('OWN'),
                "groupby": lambda : pandasDfos.groupby(df_col_grade),
                "sort": lambda : pandasDfos.sort('loan_amnt'),
                "mean": lambda : pandasDfos.mean(df_col_loan_amount),
                "drop": lambda : pandasDfos.drop('loan_amnt'),
                "dropna": pandasDfos.dropna,
                "fillna": pandasDfos.fillna,
                "concat": lambda : pandasDfos.concat(df_col_grade),
                "merge": lambda : pandasDfos.merge(df_cols_term__int_rate, df_cols_term__installment, 'term'),
            },
            # "Modin": {
            #    "isna": modinDfos.isna,
            #     "replace": lambda : modinDfos.replace('OWN'),
            #     "groupby": lambda : modinDfos.groupby(mdf_col_grade),
            #     "sort": lambda : modinDfos.sort('loan_amnt'),
            #     "mean": lambda : modinDfos.mean(mdf_col_loan_amount),
            #     "drop": lambda : modinDfos.drop('loan_amnt'),
            #     "dropna": modinDfos.dropna,
            #     "fillna": modinDfos.fillna,
            #     "concat": lambda : modinDfos.concat(mdf_col_grade),
            #     "merge": lambda : modinDfos.merge(mdf_cols_term__int_rate, mdf_cols_term__installment, 'term'),
            # },
            "Polars": {
                "isna": polarsDfos.isna,
                "replace": lambda : polarsDfos.replace('OWN'),
                "groupby": lambda : polarsDfos.groupby(pdf_col_grade),
                "sort": lambda : polarsDfos.sort('loan_amnt'),
                "mean": lambda : polarsDfos.mean(pdf_col_loan_amount),
                "drop": lambda : polarsDfos.drop('loan_amnt'),
                "dropna": polarsDfos.dropna,
                "fillna": polarsDfos.fillna,
                "concat": lambda : polarsDfos.concat(pdf_col_grade_as_renamed),
                "merge": lambda : polarsDfos.merge(pdf_cols_term__int_rate, pdf_cols_term__installment, 'term'),
            },
            "Dask": {
                "isna": daskDfos.isna,
                "replace": lambda : daskDfos.replace('OWN'),
                "groupby": lambda : daskDfos.groupby(ddf_col_grade),
                "sort": lambda : daskDfos.sort('loan_amnt'),
                "mean": lambda : daskDfos.mean(ddf_col_loan_amount),
                "drop": lambda : daskDfos.drop('loan_amnt'),
                "dropna": daskDfos.dropna,
                "fillna": daskDfos.fillna,
                "concat": lambda : daskDfos.concat(ddf_col_grade),
                "merge": lambda : daskDfos.merge(ddf_cols_term__int_rate, ddf_cols_term__installment, 'term'),
            }
        }

    def create_run_table_model(self) -> RunTableModel:
        """Create and return the run_table model here. A run_table is a List (rows) of tuples (columns),
        representing each run performed"""
        factor1 = FactorModel("Library", ['Pandas', 'Polars', 'Dask'])
        factor2 = FactorModel("DataFrame size", ['Small', 'Large'])
        subject = FactorModel("DFO", ['isna', 'replace', 'groupby', 'sort', 'mean', 'drop', 'dropna', 'fillna', 'concat', 'merge'])

        self.run_table_model = RunTableModel(
            factors=[subject, factor1, factor2],
            repetitions = 1,
            data_columns=['execution_time', 'executions_per_minute']
        )
        return self.run_table_model

    def before_experiment(self) -> None:
        """Perform any activity required before starting the experiment here
        Invoked only once during the lifetime of the program."""

        output.console_log("Config.before_experiment() called!")

    def before_run(self) -> None:
        """Perform any activity required before starting a run.
        No context is available here as the run is not yet active (BEFORE RUN)"""

        output.console_log("Config.before_run() called!")

    def start_run(self, context: RunnerContext) -> None:
        """Perform any activity required for starting the run here.
        For example, starting the target system to measure.
        Activities after starting the run should also be performed here."""
        output.console_log("Config.start_run() called!")

    def start_measurement(self, context: RunnerContext) -> None:
        """Perform any activity required for starting measurements."""
        output.console_log("Config.start_measurement() called!")
        self.start_time = time.time_ns()
        
    def run_command_on_pc(command):
        # SSH connection details
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Connect to PC via SSH
        ssh.connect('your-pc-ip', username='your-username', password='your-password')
        
        # Execute the EnergyBridge-related command
        stdin, stdout, stderr = ssh.exec_command(command)
        
        # Print command output
        output = stdout.read().decode()
        print(f"Command Output: {output}")
        
        # Close the connection
        ssh.close()
        return output

    
    def interact(self, context: RunnerContext) -> None:
        """Perform any interaction with the running target system here, or block here until the target finishes."""
        output.console_log(context.run_variation['Library'])
        output.console_log(context.run_variation['DFO'])
        # output.console_log(self.functions[context.run_variation['Library']])
        try:
            self.functions[context.run_variation['Library']][context.run_variation['DFO']]()
            output.console_log('success!')
        except Exception as e:
            output.console_log(e)


        # output.console_log("Config.interact() called with library: ", context.run_variation['Library'], " and DFO: ", context.run_variation['DFO'])

        # output.console_log(self.functions[context.run_variation['Library']][context.run_variation['DFO']]())

    def stop_measurement(self, context: RunnerContext) -> None:
        """Perform any activity here required for stopping measurements."""

        output.console_log("Config.stop_measurement called!")
        context.run_variation['execution_time'] = time.time_ns() - self.start_time
        context.run_variation['executions_per_minute'] = math.floor(60000000000 / context.run_variation['execution_time'])

    def stop_run(self, context: RunnerContext) -> None:
        """Perform any activity here required for stopping the run.
        Activities after stopping the run should also be performed here."""

        output.console_log("Config.stop_run() called!")

    def populate_run_data(self, context: RunnerContext) -> Optional[Dict[str, SupportsStr]]:
        """Parse and process any measurement data here.
        You can also store the raw measurement data under `context.run_dir`
        Returns a dictionary with keys `self.run_table_model.data_columns` and their values populated"""

        output.console_log("Config.populate_run_data() called!")
        return None

    def after_experiment(self) -> None:
        """Perform any activity required after stopping the experiment here
        Invoked only once during the lifetime of the program."""

        output.console_log("Config.after_experiment() called!")

    # ================================ DO NOT ALTER BELOW THIS LINE ================================
    experiment_path:            Path             = None
