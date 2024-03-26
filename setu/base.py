from abc import ABC,abstractmethod
from pyspark.sql.functions import rand
from argparse import Namespace,ArgumentParser
from pyspark.sql import DataFrame
from math import ceil
from typing import Callable

class SetuStage(ABC):
    def __init__(self,config:Namespace) -> None:
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
        
        self.df_total_rows = None
    
    @staticmethod
    @abstractmethod
    def add_cmdline_args(parser:ArgumentParser):
        pass

    @classmethod
    def get_stage_name(cls:ABC) -> str:
        return cls.__name__
    
    def salting(self,df:DataFrame,n_splits:int) -> DataFrame:
        num_buckets = n_splits

        df = df.withColumn("salt",(rand()*num_buckets).cast("int"))

        df = df.repartition(n_splits,"salt")

        df = df.drop("salt")

        return df
    
    def set_split_count_and_salt(self,df:DataFrame,docs_per_partition:int) -> DataFrame:
        if not self.df_total_rows:
            self.df_total_rows = df.count()

        self.n_splits = ceil(self.df_total_rows/docs_per_partition)

        df = self.salting(df,self.n_splits)

        return df
    
    @abstractmethod
    def run_stage_parallelized(self, **kwargs) -> None:
        pass

    @abstractmethod
    def run_data_parallelized(self, **kwargs) -> None:
        pass

    @abstractmethod
    def run_spark(self, parallel_mode="stage", **kwargs) -> Callable | Exception:
        if parallel_mode == "stage":
            return self.run_stage_parallelized(**kwargs)
        elif parallel_mode == "data":
            return self.run_data_parallelized(**kwargs)
        else:
            raise Exception("Incorrect input for `parallel_mode`. `parallel_mode` only supports 2 types: `stage` & `data`.")

    @abstractmethod
    def run_normal(self, **kwargs) -> None:
        pass

    @abstractmethod
    def run(self, parallel_mode="stage", **kwargs) -> Callable:
        if self.config.use_spark and self.spark_present:
            return self.run_spark(parallel_mode=parallel_mode, **kwargs)
        else:
            return self.run_normal(**kwargs)
    
class SetuComponent(ABC):
    def __init__(self,config:Namespace) -> None:
        self.config = config
        self.name = self.get_component_name()

    @classmethod
    @abstractmethod
    def add_cmdline_args(parser:ArgumentParser) -> None:
        pass
    
    @classmethod
    def get_component_name(cls:ABC) -> str:
        return cls.__name__

    @classmethod
    @abstractmethod
    def get_stage_mapping(cls) -> dict:
        pass

    @abstractmethod
    def run_stage(self, **kwargs) -> None:
        pass

    @abstractmethod
    def run(self, **kwargs) -> None:
        pass