import argparse
from core import SetuComponent
from .format_conversion import FormatConversionStage
from .page_analysis import PageAnalysisStage
from .page_filtering import PageFilteringStage
from .page_text_extraction import PageTextExtractionStage
from .page_merging import PageMergeStage

class OCRPostProcessingComponent(SetuComponent):

    def __init__(self, config, mode="crawl"):
        super().__init__(config)
        self.fc_stage = FormatConversionStage(self.config)
        self.pa_stage = PageAnalysisStage(self.config)
        self.pf_stage = PageFilteringStage(self.config)
        self.pte_stage = PageTextExtractionStage(self.config)
        self.pm_stage = PageMergeStage(self.config)

        self.stages = {
            self.fc_stage.name: self.fc_stage,
            self.pa_stage.name: self.pa_stage,
            self.pf_stage.name: self.pf_stage,
            self.pte_stage.name: self.pte_stage,
            self.pm_stage.name: self.pm_stage,
        }

    @classmethod
    def add_cmdline_args(cls, parser, for_full_pipeline=False):

        if for_full_pipeline:
            parser = FormatConversionStage.add_cmdline_args(parser)
            parser = PageAnalysisStage.add_cmdline_args(parser)
            parser = PageFilteringStage.add_cmdline_args(parser)
            parser = PageTextExtractionStage.add_cmdline_args(parser)
            parser = PageMergeStage.add_cmdline_args(parser)

        else:
            fc_subparser = parser.add_parser(FormatConversionStage.get_stage_name(), help='Run format conversion stage of ocr postprocessing component')
            fc_subparser = FormatConversionStage.add_cmdline_args(fc_subparser)

            pa_subparser = parser.add_parser(PageAnalysisStage.get_stage_name(), help='Run page analysis stage of ocr postprocessing component')
            pa_subparser = PageAnalysisStage.add_cmdline_args(pa_subparser)

            pf_subparser = parser.add_parser(PageFilteringStage.get_stage_name(), help='Run page filtering stage of ocr postprocessing component')
            pf_subparser = PageFilteringStage.add_cmdline_args(pf_subparser)

            pte_subparser = parser.add_parser(PageTextExtractionStage.get_stage_name(), help='Run page text extraction stage of ocr postprocessing component')
            pte_subparser = PageTextExtractionStage.add_cmdline_args(pte_subparser)

            pm_subparser = parser.add_parser(PageMergeStage.get_stage_name(), help='Run page merging stage of ocr postprocessing component')
            pm_subparser = PageMergeStage.add_cmdline_args(pm_subparser)

            ocr_parser = parser.add_parser(cls.get_component_name(), help='Run entire ocr postprocessing component')
            ocr_parser = FormatConversionStage.add_cmdline_args(ocr_parser)
            ocr_parser = PageAnalysisStage.add_cmdline_args(ocr_parser)
            ocr_parser = PageFilteringStage.add_cmdline_args(ocr_parser)
            ocr_parser = PageTextExtractionStage.add_cmdline_args(ocr_parser)
            ocr_parser = PageMergeStage.add_cmdline_args(ocr_parser)

        return parser

    @classmethod
    def get_stage_mapping(cls):
        return dict.fromkeys([
            FormatConversionStage.get_stage_name(), 
            PageAnalysisStage.get_stage_name(), 
            PageFilteringStage.get_stage_name(), 
            PageTextExtractionStage.get_stage_name(), 
            PageMergeStage.get_stage_name(), 
            cls.get_component_name()
        ], cls.get_component_name())

    def run_stage(self, spark, stage, **kwargs):
        return self.stages[stage].run(spark, **kwargs)

    def run(self, spark, stage, **kwargs):
        if stage == self.name:
            raise NotImplementedError(f"`run` function for running the entire component has not been implemented for class `{self.name}`")
        else:
            return self.run_stage(spark=spark, stage=stage, **kwargs)