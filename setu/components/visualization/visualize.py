import argparse
from core import SetuComponent
from .j2p_viz import VisualizeJson2ParquetStage
from .doc_clean_viz import VisualizeDocCleanStage
from .lid_viz import VisualizeLIDStage
from .analysis_viz import VisualizeAnalysisStage
from .filtering_viz import VisualizeFilterStage
from .minhash_viz import VisualizeMinhashStage

class VisualizationComponent(SetuComponent):

    def __init__(self, config):
        super().__init__(config)
    
        self.viz_j2p = VisualizeJson2ParquetStage(self.config)
        self.viz_dc = VisualizeDocCleanStage(self.config)
        self.viz_lid = VisualizeLIDStage(self.config)
        self.viz_ana = VisualizeAnalysisStage(self.config)
        self.viz_fil = VisualizeFilterStage(self.config)
        self.viz_min = VisualizeMinhashStage(self.config)

        self.stages = {
            self.viz_j2p.name: self.viz_j2p,
            self.viz_dc.name: self.viz_dc,
            self.viz_lid.name: self.viz_lid,
            self.viz_ana.name: self.viz_ana,
            self.viz_fil.name: self.viz_fil,
            self.viz_min.name: self.viz_min,
        }

    @classmethod
    def add_cmdline_args(cls, parser, for_full_pipeline=False): 

        if for_full_pipeline:
            parser = VisualizeJson2ParquetStage.add_cmdline_args(parser)
            parser = VisualizeDocCleanStage.add_cmdline_args(parser)
            parser = VisualizeLIDStage.add_cmdline_args(parser)
            parser = VisualizeAnalysisStage.add_cmdline_args(parser)
            parser = VisualizeFilterStage.add_cmdline_args(parser)
            parser = VisualizeMinhashStage.add_cmdline_args(parser)
        else:

            viz_j2p_subparser = parser.add_parser(VisualizeJson2ParquetStage.get_stage_name(), help="Run JSON 2 Parquet Visualization Stage of visualization component")
            viz_j2p_subparser = VisualizeJson2ParquetStage.add_cmdline_args(viz_j2p_subparser)

            viz_dc_subparser = parser.add_parser(VisualizeDocCleanStage.get_stage_name(), help='Run Document Cleaning Visualization stage of visualization component')
            viz_dc_subparser = VisualizeDocCleanStage.add_cmdline_args(viz_dc_subparser)

            viz_lid_subparser = parser.add_parser(VisualizeLIDStage.get_stage_name(), help='Run LID Visualization stage of visualization component')
            viz_lid_subparser = VisualizeLIDStage.add_cmdline_args(viz_lid_subparser)

            viz_ana_subparser = parser.add_parser(VisualizeAnalysisStage.get_stage_name(), help='Run Analysis Visualization stage of visualization component')
            viz_ana_subparser = VisualizeAnalysisStage.add_cmdline_args(viz_ana_subparser)

            viz_fil_subparser = parser.add_parser(VisualizeFilterStage.get_stage_name(), help='Run Filtering Visualization stage of visualization component')
            viz_fil_subparser = VisualizeFilterStage.add_cmdline_args(viz_fil_subparser)

            viz_min_subparser = parser.add_parser(VisualizeMinhashStage.get_stage_name(), help='Run Minhash Visualization stage of visualization component')
            viz_min_subparser = VisualizeMinhashStage.add_cmdline_args(viz_min_subparser)

            viz_com_parser = parser.add_parser(cls.get_component_name(), help='Run entire Visualization component')
            viz_com_parser = VisualizeJson2ParquetStage.add_cmdline_args(viz_com_parser)
            viz_com_parser = VisualizeDocCleanStage.add_cmdline_args(viz_com_parser)
            viz_com_parser = VisualizeLIDStage.add_cmdline_args(viz_com_parser)
            viz_com_parser = VisualizeAnalysisStage.add_cmdline_args(viz_com_parser)
            viz_com_parser = VisualizeFilterStage.add_cmdline_args(viz_com_parser)
            viz_com_parser = VisualizeMinhashStage.add_cmdline_args(viz_com_parser)

        return parser

    @classmethod
    def get_stage_mapping(cls):
        return dict.fromkeys([
            VisualizeJson2ParquetStage.get_stage_name(),
            VisualizeDocCleanStage.get_stage_name(), 
            VisualizeLIDStage.get_stage_name(), 
            VisualizeAnalysisStage.get_stage_name(), 
            VisualizeFilterStage.get_stage_name(),
            VisualizeMinhashStage.get_stage_name(),
            cls.get_component_name()
        ], 
        cls.get_component_name())

    def run_stage(self, spark, stage, **kwargs):
        return self.stages[stage].run(spark, **kwargs)

    def run(self, spark, stage, **kwargs):
        if stage == self.name:
            raise NotImplementedError("`run` function for running the entire component has not been implemented for class `VisualizationComponent`")
        else:
            return self.run_stage(spark=spark, stage=stage, **kwargs)