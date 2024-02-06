import argparse
from core import SetuComponent
from .custom_join import CustomJoinStage
from .page2pdf import Page2PDFStage
from .normalize import NormalizeStage

class UtilityComponent(SetuComponent):

    def __init__(self, config, mode="crawl"):
        super().__init__(config)
        self.cj_stage = CustomJoinStage(self.config)
        self.p2p_stage = Page2PDFStage(self.config)
        self.n_stage = NormalizeStage(self.config)

        self.stages = {
            self.cj_stage.name: self.cj_stage,
            self.p2p_stage.name: self.p2p_stage,
            self.n_stage.name: self.n_stage,
        }

    @classmethod
    def add_cmdline_args(cls, parser):

        cj_subparser = parser.add_parser(CustomJoinStage.get_stage_name(), help='Run custom join stage of utility component')
        cj_subparser = CustomJoinStage.add_cmdline_args(cj_subparser)

        p2p_subparser = parser.add_parser(Page2PDFStage.get_stage_name(), help='Run page2pdf stage of utility component')
        p2p_subparser = Page2PDFStage.add_cmdline_args(p2p_subparser)

        n_subparser = parser.add_parser(NormalizeStage.get_stage_name(), help="Run normalize stage of utility component.")
        n_subparser = NormalizeStage.add_cmdline_args(n_subparser)

        return parser

    @classmethod
    def get_stage_mapping(cls):
        return dict.fromkeys([
            CustomJoinStage.get_stage_name(), 
            Page2PDFStage.get_stage_name(),
            NormalizeStage.get_stage_name(),
            cls.get_component_name()
        ], cls.get_component_name())

    def run_stage(self, spark, stage, **kwargs):
        return self.stages[stage].run(spark, **kwargs)

    def run(self, spark, stage, **kwargs):
        if stage == self.name:
            raise NotImplementedError(f"`run` function for running the entire component has not been implemented for class `{self.name}`")
        else:
            return self.run_stage(spark=spark, stage=stage, **kwargs)