import argparse
from core import SetuComponent
from .doc_clean import DocCleanStage
from .lid import LIDStage
from .analysis import AnalysisStage

class CrawlAnalysisComponent(SetuComponent):

    def __init__(self, config):
        super().__init__(config)
    
        self.doc_clean_stage = DocCleanStage(self.config)
        self.lid_stage = LIDStage(self.config)
        self.analysis_stage = AnalysisStage(self.config)

        self.stages = {
            self.doc_clean_stage.name: self.doc_clean_stage,
            self.lid_stage.name: self.lid_stage,
            self.analysis_stage.name: self.analysis_stage
        }

    @classmethod
    def add_cmdline_args(cls, parser, for_full_pipeline=False): 

        if for_full_pipeline:
            parser = DocCleanStage.add_cmdline_args(parser)
            parser = LIDStage.add_cmdline_args(parser)
            parser = AnalysisStage.add_cmdline_args(parser)
        else:
            dc_subparser = parser.add_parser(DocCleanStage.get_stage_name(), help='Run document cleaning stage of crawl analysis component')
            dc_subparser = DocCleanStage.add_cmdline_args(dc_subparser)

            lid_subparser = parser.add_parser(LIDStage.get_stage_name(), help='Run lid stage of crawl analysis component')
            lid_subparser = LIDStage.add_cmdline_args(lid_subparser)

            aly_subparser = parser.add_parser(AnalysisStage.get_stage_name(), help='Run analysis stage of crawl analysis component')
            aly_subparser = AnalysisStage.add_cmdline_args(aly_subparser)

            ca_parser = parser.add_parser(cls.get_component_name(), help='Run entire crawl analysis component')
            ca_parser = DocCleanStage.add_cmdline_args(ca_parser)
            ca_parser = LIDStage.add_cmdline_args(ca_parser)
            ca_parser = AnalysisStage.add_cmdline_args(ca_parser)

        return parser

    @classmethod
    def get_stage_mapping(cls):
        return dict.fromkeys([
            DocCleanStage.get_stage_name(), 
            LIDStage.get_stage_name(), 
            AnalysisStage.get_stage_name(),
            cls.get_component_name()
        ], 
        cls.get_component_name())

    def run_stage(self, spark, stage, **kwargs):
        return self.stages[stage].run(spark, **kwargs)

    def run(self, spark, stage, **kwargs):
        if stage == self.name:
            raise NotImplementedError("`run` function for running the entire component has not been implemented for class `CrawlAnalysisComponent`")
        else:
            return self.run_stage(spark=spark, stage=stage, **kwargs)