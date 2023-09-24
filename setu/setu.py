import argparse
from argparse import Namespace
from core import (
    CONSTANTS,
    KW_PROCESSORS,
    str2bool
)
import json
from components.text_extraction import TextExtractionComponent
from components.crawl_analysis import CrawlAnalysisComponent
from components.flagging_and_removal import FlagAndRemoveComponent

class Setu():

    def __init__(self, config_file, source_mode="crawl"):
        
        self.config = self.load_config(config_file)

        self.name = self.get_pipeline_name()

        self._stage_component_mapping = self.get_stage_component_mapping()

        self.te_component = TextExtractionComponent(self.config, mode=source_mode)
        self.ca_component = CrawlAnalysisComponent(self.config)
        self.fr_component = FlagAndRemoveComponent(self.config)
        self.components = {
            self.te_component.name: self.te_component,
            self.ca_component.name: self.ca_component,
            self.fr_component.name: self.fr_component,
        }
        
    def get_component_list():
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

        stage_parser = parser.add_subparsers(dest="stage", help='Indicate what stage to run inside a component')
        
        stage_parser = TextExtractionComponent.add_cmdline_args(stage_parser)
        stage_parser = CrawlAnalysisComponent.add_cmdline_args(stage_parser)
        stage_parser = FlagAndRemoveComponent.add_cmdline_args(stage_parser)

        stage_parser = stage_parser.add_parser(cls.get_pipeline_name(), help=f'Run complete end-to-end {cls.get_pipeline_name()}')
        stage_parser = TextExtractionComponent.add_cmdline_args(stage_parser, for_full_pipeline=True)
        stage_parser = CrawlAnalysisComponent.add_cmdline_args(stage_parser, for_full_pipeline=True)
        stage_parser = FlagAndRemoveComponent.add_cmdline_args(stage_parser, for_full_pipeline=True)

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
            CrawlAnalysisComponent,
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
            subprocess.run(["gsutil", "cp", config_file, tmp_location])

        with open(tmp_location, "r") as config_f:
            config_= json.load(config_f)
        return Namespace(**config_)

    def run_component(self, spark, component, **kwargs):
        return self.components[self._stage_component_mapping[component]].run(spark=spark, stage=component, **kwargs)

    def run(self, spark, component, **kwargs):
        if component == self.name:
            raise NotImplementedError("`run` function for running the entire pipeline has not been implemented for class `Setu`")
        else:
            return self.run_component(spark=spark, component=component, **kwargs)


if __name__ == "__main__":

    setu = Setu(config_file="/mnt/phallm-data/priyam/setu/pipeline/configs/dashboard_config.json")

    # TODO: a dummy of the pipeline on a single example.