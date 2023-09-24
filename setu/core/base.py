from pyspark.sql import SparkSession
from pyspark import TaskContext
from pyspark.sql.functions import (
    udf,
    posexplode,
    col,
    rand
)
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

class SetuStage(ABC):

    def __init__(self, config):
        self.config = config
        self.name = self.get_stage_name()
        if self.config.use_spark:
            self.spark_present = True
            try:
                import pyspark
                print(f"PySpark Installed. Going forward with spark execution for {self.__class__.__name__}")
            except ImportError as e:
                print(f"PySpark not present. Falling back to normal execution for {self.__class__.__name__}")
                self.spark_present = False

    @staticmethod
    @abstractmethod
    def add_cmdline_args(parser):
        pass

    @classmethod
    def get_stage_name(cls):
        return cls.__name__

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
    def run_stage_parallelized(self, **kwargs):
        pass

    @abstractmethod
    def run_data_parallelized(self, **kwargs):
        pass

    @abstractmethod
    def run_spark(self, parallel_mode="stage", **kwargs):
        if parallel_mode == "stage":
            return self.run_stage_parallelized(**kwargs)
        elif parallel_mode == "data":
            return self.run_data_parallelized(**kwargs)
        else:
            raise Exception("Incorrect input for `parallel_mode`. `parallel_mode` only supports 2 types: `stage` & `data`.")

    @abstractmethod
    def run_normal(self, **kwargs):
        pass

    @abstractmethod
    def run(self, parallel_mode="stage", **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(parallel_mode=parallel_mode, **kwargs)
        else:
            return self.run_normal(**kwargs)


class SetuComponent(ABC):

    def __init__(self, config):
        self.config = config
        self.name = self.get_component_name()

    @classmethod
    @abstractmethod
    def add_cmdline_args(parser):
        pass
    
    @classmethod
    def get_component_name(cls):
        return cls.__name__

    @classmethod
    @abstractmethod
    def get_stage_mapping(cls):
        pass

    @abstractmethod
    def run_stage(self, **kwargs):
        pass

    @abstractmethod
    def run(self, **kwargs):
        pass
