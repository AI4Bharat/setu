from typing import Dict
import numpy as np
from pyspark.sql import SparkSession
from pyspark import TaskContext
from argparse import Namespace
import pyarrow as pa
import pyarrow.parquet as pq
import argparse
import os
from math import ceil
import time
import pandas as pd
import subprocess

from abc import ABC, abstractmethod

class SetuComponent(ABC):

    def __init__(self, config):
        self.config = config

    def salting(self, df, n_splits):
        # Number of salt buckets
        num_buckets = n_splits  # Adjust based on your data size and level of skewness

        # Adding a salt column to the DataFrame
        df = df.withColumn("salt", (rand() * num_buckets).cast("int"))

        # df.show(1000)

        # Repartition based on the salted key to ensure more even distribution
        df = df.repartition(n_splits, "salt")

        df = df.drop("salt")

        return df

    def set_split_count_and_salt(self, df, docs_per_partition):

        self.df_total_rows = df.count()

        self.n_splits = ceil(self.df_total_rows/docs_per_partition)

        print(f"When required data will be repartitioned into - {self.n_splits} partitions")

        df = self.salting(df, self.n_splits)

        return df

    @abstractmethod
    def run_stage_parallelized(self):
        pass

    @abstractmethod
    def run_data_parallelized(self):
        pass

    def run(self, parallel_mode="stage"):
        if parallel_mode == "stage":
            return self.run_stage_parallelized()
        elif parallel_mode == "data":
            return self.run_data_parallelized()
        else:
            raise Exception("Incorrect input for `parallel_mode`. `parallel_mode` only supports 2 types: `stage` & `data`.")
            