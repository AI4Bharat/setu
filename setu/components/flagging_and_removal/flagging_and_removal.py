import argparse
from core import SetuComponent
from .flagging_and_filtering import FlaggingAndFilteringStage
from .document_removal import DocumentRemovalStage

class FlagAndRemoveComponent(SetuComponent):

    def __init__(self, config):
        super().__init__(config)

        self.ff_stage = FlaggingAndFilteringStage(self.config)
        self.doc_removal_stage = DocumentRemovalStage(self.config)

        self.stages = {
            self.ff_stage.name: self.ff_stage,
            self.doc_removal_stage.name: self.doc_removal_stage,
        }

    @classmethod
    def add_cmdline_args(cls, parser, for_full_pipeline=False): 

        if for_full_pipeline:
            parser = FlaggingAndFilteringStage.add_cmdline_args(parser)
            parser = DocumentRemovalStage.add_cmdline_args(parser)
        else:
            fnf_subparser = parser.add_parser(FlaggingAndFilteringStage.get_stage_name(), help='Run flagging and filtering stage of flag-filter component')
            fnf_subparser = FlaggingAndFilteringStage.add_cmdline_args(fnf_subparser)

            dr_subparser = parser.add_parser(DocumentRemovalStage.get_stage_name(), help='Run document removal stage of flag-filter component')
            dr_subparser = DocumentRemovalStage.add_cmdline_args(dr_subparser)

            fnr_parser = parser.add_parser(cls.get_component_name(), help='Run entire flagging and removal component')
            fnr_parser = FlaggingAndFilteringStage.add_cmdline_args(fnr_parser)
            fnr_parser = DocumentRemovalStage.add_cmdline_args(fnr_parser)

        return parser

    @classmethod
    def get_stage_mapping(cls):
        return dict.fromkeys([
            FlaggingAndFilteringStage.get_stage_name(), 
            DocumentRemovalStage.get_stage_name(), 
            cls.get_component_name()
        ], cls.get_component_name())

    def run_stage(self, spark, stage, **kwargs):
        return self.stages[stage].run(spark, **kwargs)

    def run(self, spark, stage, **kwargs):
        if stage == self.name:
            raise NotImplementedError("`run` function for running the entire component has not been implemented for class `FlagAndRemoveComponent`")
        else:
            return self.run_stage(spark=spark, stage=stage, **kwargs)