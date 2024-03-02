import argparse
from core import SetuComponent
from .json_to_parquet import JSON2ParquetStage
from .extract_text import ExtractTextStage

class TextExtractionComponent(SetuComponent):

    def __init__(self, config, mode="crawl"):
        super().__init__(config)
        self.j2p_stage = JSON2ParquetStage(self.config, mode)
        self.te_stage = ExtractTextStage(self.config)

        self.stages = {
            self.j2p_stage.name: self.j2p_stage,
            self.te_stage.name: self.te_stage,
        }

    @classmethod
    def add_cmdline_args(cls, parser, for_full_pipeline=False):

        if for_full_pipeline:
            parser = JSON2ParquetStage.add_cmdline_args(parser)
            parser = ExtractTextStage.add_cmdline_args(parser)
        else:
            j2p_subparser = parser.add_parser(JSON2ParquetStage.get_stage_name(), help='Run json 2 parquet stage of text extraction component')
            j2p_subparser = JSON2ParquetStage.add_cmdline_args(j2p_subparser)

            te_core_subparser = parser.add_parser(ExtractTextStage.get_stage_name(), help='Run core text extraction stage of text extraction component')
            te_core_subparser = ExtractTextStage.add_cmdline_args(te_core_subparser)

            te_parser = parser.add_parser(cls.get_component_name(), help='Run entire text-extraction component')
            te_parser = JSON2ParquetStage.add_cmdline_args(te_parser)
            te_parser = ExtractTextStage.add_cmdline_args(te_parser)

        return parser

    @classmethod
    def get_stage_mapping(cls):
        return dict.fromkeys([
            JSON2ParquetStage.get_stage_name(), 
            ExtractTextStage.get_stage_name(), 
            cls.get_component_name()
        ], cls.get_component_name())

    def run_stage(self, spark, stage, **kwargs):
        return self.stages[stage].run(spark, **kwargs)

    def run(self, spark, stage, **kwargs):
        if stage == self.name:
            raise NotImplementedError("`run` function for running the entire component has not been implemented for class `TextExtractionComponent`")
        else:
            return self.run_stage(spark=spark, stage=stage, **kwargs)