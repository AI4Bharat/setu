import argparse
import os
import json
import subprocess
from argparse import Namespace,ArgumentParser
from utilities import str2bool
from typing import Callable
from text_extraction import TextExtractionComponent
from clean_analysis import CleanAnalysisComponent
from flagging_and_removal import FlagAndRemoveComponent
from pyspark.sql import SparkSession

class Setu():

    """ Main Setu Class which defines the pipeline object.
    """

    def __init__(self,config_file:str,source_mode="crawl") -> None:
        """__init__ Method that initializes the Setu Class object given the configuration path and data source type.

        Args:
            config_file (str): Path to the configuration file based on the language and data input type.
            source_mode (str, optional): Type of the data input
        """
        self.config = self.load_config(config_file)
        """Configuration Namespace object for the pipeline"""

        self.name = self.get_pipeline_name()
        """Name of the pipeline"""

        self._stage_component_mapping = self.get_stage_component_mapping()
        """The Stage-Component mapping for the pipeline"""

        self.te_component = TextExtractionComponent(self.config, mode=source_mode)
        """The TextExtraction Component of the pipeline."""
        self.ca_component = CleanAnalysisComponent(self.config)
        """The CleanAnalysis Component of the pipeline."""
        self.fr_component = FlagAndRemoveComponent(self.config)
        """The FlagAndRemove Component of the pipeline"""

        self.components = {
            self.te_component.name : self.te_component,
            self.ca_component.name: self.ca_component,
            self.fr_component.name: self.fr_component,
        }
        """The dictionary mapping of the different components and their names."""

    def get_component_list() -> list:
        """get_component_list Method that fetches a list of all the component names in the pipeline.

        Returns:
            list: A list of all the component names present as part of the pipeline.
        """
        return list(self.components.keys())
    
    @classmethod
    def parse_args(cls) -> Namespace:
        """parse_args Method that parses the main and sub arguments required to execute the various stages and components.

        Returns:
            Namespace: A Namespace object containing the required arguments for the pipeline.
        """
        parser = argparse.ArgumentParser(description="Setu: a spark based data-curation pipeline")

        parser.add_argument(
            "--config",
            type=str,
            required=True,
            help="Path to config file",
        )

        parser.add_argument(
            "--mode",
            type=str,
            choices=["crawl", "ocr", "asr"],
            required=False,
            default="crawl",
            help="Input data source",
        )

        parser.add_argument(
            "--run_local",
            type=str2bool,
            required=False,
            default=False,
            help="Whether pipeline is running locally or on a cloud cluster",
        )

        stage_parser = parser.add_subparsers(dest="stage", help='Indicate what stage to run inside a component',required=False)
        stage_parser = TextExtractionComponent.add_cmdline_args(stage_parser)
        stage_parser = CleanAnalysisComponent.add_cmdline_args(stage_parser)
        stage_parser = FlagAndRemoveComponent.add_cmdline_args(stage_parser)

        stage_parser = stage_parser.add_parser(cls.get_pipeline_name(), help=f'Run complete end-to-end {cls.get_pipeline_name()}')
        stage_parser = TextExtractionComponent.add_cmdline_args(stage_parser,for_full_pipeline=True)
        stage_parser = CleanAnalysisComponent.add_cmdline_args(stage_parser,for_full_pipeline=True)
        stage_parser = FlagAndRemoveComponent.add_cmdline_args(stage_parser,for_full_pipeline=True)
        args = parser.parse_args()
        
        return args
    
    
    @classmethod
    def get_pipeline_name(cls)->str:
        """get_pipeline_name Method that returns the pipeline name.

        Returns:
            str: Name of the pipeline.
        """
        return cls.__name__
    

    @classmethod
    def get_stage_component_mapping(cls) -> dict:
        """get_stage_component_mapping Method that returns the different stages mapped to their respective components.

        Returns:
            dict: A dictionary containing the different components and their stages.
        """
        stage_component_map = {}
        for component in [
            TextExtractionComponent,
            CleanAnalysisComponent,
            FlagAndRemoveComponent
        ]:
            stage_component_map = stage_component_map | component.get_stage_mapping()

        stage_component_map = stage_component_map | {cls.get_pipeline_name(): cls.get_pipeline_name()}
        return stage_component_map

    @staticmethod
    def load_config(config_file:str)->Namespace:
        """load_config Method that loads the configuration variable from the config file path.

        Args:
            config_file (str): Path to the configuration file.

        Returns:
            Namespace: Configuration Namespace object.
        """
        tmp_location = config_file

        if config_file.startswith("gs://"):
            tmp_location = "/tmp/" + os.path.split(config_file)[1]

            # Refer to this: https://stackoverflow.com/a/45991873
            subprocess.run(["gsutil", "cp", config_file, tmp_location])

        with open(tmp_location, "r") as config_f:
            config_= json.load(config_f)
        return Namespace(**config_)
    
    def run_component(self, spark:SparkSession, component:str, **kwargs)->Callable:
        """run_component Method that executes the particular component given the spark context object

        Args:
            spark (SparkSession): The current spark session object.
            component (str): Name of the component to execute. Setu if execute whole pipeline.

        Returns:
            Callable: Returns the run function of the particular component
        """
        return self.components[self._stage_component_mapping[component]].run(spark=spark, stage=component, **kwargs)

    def run(self, spark:SparkSession, component:str, **kwargs)->Callable:
        """run Method that executes the pipeline.

        Args:
            spark (SparkSession): The current spark session object.
            component (str): Name of the component to execute. Setu if execute whole pipeline.

        Returns:
            Callable: Returns the run_component function of the particular component
        """
        if component == self.name:
            for subcomponent in ["TextExtractionComponent","CleanAnalysisComponent","FlagAndRemoveComponent"]:
                self.run_component(spark=spark, component=subcomponent, **kwargs)
        else:
            return self.run_component(spark=spark, component=component, **kwargs)


if __name__ == "__main__":

    setu = Setu(config_file="../configs/crawls/spark_english_config.json")
    print(setu.get_stage_component_mapping())