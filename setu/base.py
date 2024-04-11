from abc import ABC,abstractmethod
from pyspark.sql.functions import rand
from argparse import Namespace,ArgumentParser
from pyspark.sql import DataFrame
from math import ceil
from typing import Callable

class SetuStage(ABC):
    """SetuStage Base Class for the different stages in Setu Pipeline

    Args:
        config (Namespace): The configuration file for the particular data type and language to be processed by the pipeline.
    """
    def __init__(self,config:Namespace) -> None:
        self.config = config
        """Configuration Namespace object for the particular stage."""
        self.name = self.get_stage_name()
        """Name of the particular stage."""
        if self.config.use_spark:
            self.spark_present = True
            try:
                import pyspark
                print(f"PySpark Installed. Going forward with spark execution for {self.__class__.__name__}")
            except ImportError as e:
                print(f"PySpark not present. Falling back to normal execution for {self.__class__.__name__}")
                self.spark_present = False
        else :
            self.spark_present = False
        self.df_total_rows = None
        """Total number of rows in the given Dataframe Object."""
    
    @staticmethod
    @abstractmethod
    def add_cmdline_args(parser:ArgumentParser):
        """add_cmdline_args Abstract Method for adding command line arguments for the particular stage

        Args:
            parser (ArgumentParser): The argument parser object to which the current stage arguments need to be added.
        """
        pass

    @classmethod
    def get_stage_name(cls:ABC) -> str:
        """get_stage_name Method to fetch the particular stage name

        Args:
            cls (ABC): The self cls of which the name should be returned

        Returns:
            str: Name of the stage
        """
        return cls.__name__
    
    def salting(self,df:DataFrame,n_splits:int) -> DataFrame:
        """salting Method which performs salting on the particular dataframe object to handle data skewness.

        Args:
            df (DataFrame): The dataframe object that is processed by the stage.
            n_splits (int): Number of splits for salting

        Returns:
            DataFrame: Returns the dataframe object after performing salting
        """
        num_buckets = n_splits

        df = df.withColumn("salt",(rand()*num_buckets).cast("int"))

        df = df.repartition(n_splits,"salt")

        df = df.drop("salt")

        return df
    
    def set_split_count_and_salt(self,df:DataFrame,docs_per_partition:int) -> DataFrame:
        """set_split_count_and_salt Method to calculate the number of splits and salting given the dataframe and number of documents per partition.

        Args:
            df (DataFrame): The dataframe object that is processed by the stage.
            docs_per_partition (int): Number of documents that each spark partition should handle.

        Returns:
            DataFrame: Returns the dataframe object after performing salting
        """
        if not self.df_total_rows:
            self.df_total_rows = df.count()

        self.n_splits = ceil(self.df_total_rows/docs_per_partition)

        df = self.salting(df,self.n_splits)

        return df
    
    @abstractmethod
    def run_stage_parallelized(self, **kwargs) -> None:
        """run_stage_parallelized Abstract method for running the stage in parallel mode.
        """
        pass

    @abstractmethod
    def run_data_parallelized(self, **kwargs) -> None:
        """run_data_parallelized Abstract method for running the stage in data parallel mode.
        """
        pass

    @abstractmethod
    def run_spark(self, parallel_mode="stage", **kwargs) -> Callable | Exception:
        """run_spark Method which triggers spark execution of the particular stage.

        Args:
            parallel_mode (str, optional): The mode in which spark should execute, either data or stage parallel. Defaults to "stage".

        Raises:
            Exception: Throws an exception when unknown value is provided for parallel_mode.

        Returns:
            Callable | Exception: Exception that indicates the user has provided an incorrect value for parallel_mode.
        """
        if parallel_mode == "stage":
            return self.run_stage_parallelized(**kwargs)
        elif parallel_mode == "data":
            return self.run_data_parallelized(**kwargs)
        else:
            raise Exception("Incorrect input for `parallel_mode`. `parallel_mode` only supports 2 types: `stage` & `data`.")

    @abstractmethod
    def run_normal(self, **kwargs) -> None:
        """run_normal Method for executing the stage in normal without Spark Utilization
        """
        pass

    @abstractmethod
    def run(self, parallel_mode="stage", **kwargs) -> Callable:
        """run Method that triggers stage execution based on values of parallel_mode and if spark should be used.

        Args:
            parallel_mode (str, optional): _description_. Defaults to "stage".

        Returns:
            Callable: _description_
        """
        if self.config.use_spark and self.spark_present:
            return self.run_spark(parallel_mode=parallel_mode, **kwargs)
        else:
            return self.run_normal(**kwargs)
    
class SetuComponent(ABC):
    """SetuComponent Base Class for the different components in Setu Pipeline

    Args:
        config (Namespace): The configuration file for the particular data type and language to be processed by the pipeline.
    """
    def __init__(self,config:Namespace) -> None:
        self.config = config
        """Configuration Namespace object for the particular component."""
        self.name = self.get_component_name()
        """Name of the particular component."""

    @classmethod
    @abstractmethod
    def add_cmdline_args(parser:ArgumentParser) -> None:
        """add_cmdline_args Abstract Method for adding command line arguments for the particular component

        Args:
            parser (ArgumentParser): The argument parser object to which the current stage arguments need to be added.
        """
        pass
    
    @classmethod
    def get_component_name(cls:ABC) -> str:
        """get_component_name Method to fetch the particular component name

        Args:
            cls (ABC): The self cls of which the name should be returned

        Returns:
            str: Name of the component
        """
        return cls.__name__

    @classmethod
    @abstractmethod
    def get_stage_mapping(cls) -> dict:
        """get_stage_mapping Method the provides the a dictionary mapping of the stage names as keys and their corresponding stage objects as values.

        Returns:
            dict: Dictionary containing the different stage names and objects as key/value pairs.
        """
        pass

    @abstractmethod
    def run_stage(self, **kwargs) -> None:
        """run_stage Method the triggers the execution of a particular stage/omponent if the stage/component name is provided.
        """
        pass

    @abstractmethod
    def run(self, **kwargs) -> None:
        """run Method that triggers the execution of the component.
        """
        pass