import argparse
import os
import json
import subprocess
from argparse import Namespace,ArgumentParser
from utilities import str2bool

from text_extraction import TextExtractionComponent
from clean_analysis import CleanAnalysisComponent
from flagging_and_removal import FlagAndRemoveComponent

class Setu():

    def __init__(self,config_file:str,source_mode="crawl",full=False) -> None:

        self.config = self.load_config(config_file)

        self.name = self.get_pipeline_name()

        self._stage_component_mapping = self.get_stage_component_mapping()

        self.te_component = TextExtractionComponent(self.config, mode=source_mode)
        self.ca_component = CleanAnalysisComponent(self.config,)
        self.fr_component = FlagAndRemoveComponent(self.config)

        self.components = {
            self.te_component.name : self.te_component,
            self.ca_component.name: self.ca_component,
            self.fr_component.name: self.fr_component,
        }

    def get_component_list():
        """
        Retrieves the list of keys (names) of the components in the pipeline.

        Returns:
            A list containing the keys of all components present in the setu pipeline.
        """
        return list(self.components.keys())
    
    @classmethod
    def parse_args(cls):

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
    def get_pipeline_name(cls):
        return cls.__name__
    

    @classmethod
    def get_stage_component_mapping(cls):
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
    def load_config(config_file):

        tmp_location = config_file

        if config_file.startswith("gs://"):
            tmp_location = "/tmp/" + os.path.split(config_file)[1]

            # Refer to this: https://stackoverflow.com/a/45991873
            subprocess.run(["gsutil", "cp", config_file, tmp_location])

        with open(tmp_location, "r") as config_f:
            config_= json.load(config_f)
        return Namespace(**config_)
    
    def run_component(self, spark, component, **kwargs):
        return self.components[self._stage_component_mapping[component]].run(spark=spark, stage=component, **kwargs)

    def run(self, spark, component, **kwargs):
        if component == self.name:
            for subcomponent in ["TextExtractionComponent","CleanAnalysisComponent","FlagAndRemoveComponent"]:
                self.run_component(spark=spark, component=subcomponent, **kwargs)
        else:
            return self.run_component(spark=spark, component=component, **kwargs)


if __name__ == "__main__":

    setu = Setu(config_file="../configs/crawls/spark_english_config.json")
    print(setu.get_stage_component_mapping())