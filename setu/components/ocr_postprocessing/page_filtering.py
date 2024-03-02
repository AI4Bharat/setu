import argparse
from core import SetuStage, str2bool
import subprocess
from pyspark.sql.functions import when

class PageFilteringStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--pf_parquets_path",
            type=str,
            help="Path of parquets for page analysis in OCR postprocessing"
        )

        parser.add_argument(
            "--pf_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--pf_run_mode",
            type=str,
            required=False,
            choices=["stage"],
            default="stage",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--pf_perform_flagging",
            type=str2bool,
            required=False,
            default=True,
            help="Perform Page Flagging?",
        )

        parser.add_argument(
            "--pf_perform_removal",
            type=str2bool,
            required=False,
            default=True,
            help="Perform Page Removal?",
        )

        parser.add_argument(
            "--pf_output_path",
            type=str,
            help="Directory where all the analysised pages will be stored."
        )
        
        return parser

    def run_stage_parallelized(
        self,
        df,
        docs_per_partition,
        perform_flagging,
        perform_removal,
        output_path,

    ):
        df = self.set_split_count_and_salt(df, docs_per_partition)

        if perform_flagging:
            df = df \
                .select("*", when(df["horizontal_coverage"] < self.config.horizontal_coverage_threshold, True).otherwise(False).alias("is_horizontally_sparse")) \
                .select("*", when(df["vertical_coverage"] < self.config.vertical_coverage_threshold, True).otherwise(False).alias("is_vertically_sparse")) \
                .select("*", when(df["max_iou"] > self.config.max_iou_threshold , True).otherwise(False).alias("is_overlapping")) \
                .select("*", when(df["script_coverage"] == False, True).otherwise(False).alias("is_script_unconfident")) \
                .select("*", when(df["min_block_density"] < self.config.block_density_threshold, True).otherwise(False).alias("contains_sparse_blocks"))

        if perform_removal:
            if self.config.remove_horizontally_sparse:
                df = df.filter(df.is_horizontally_sparse == False)
            if self.config.remove_vertically_sparse:
                df = df.filter(df.is_vertically_sparse == False)
            if self.config.remove_overlapping:
                df = df.filter(df.is_overlapping == False)
            if self.config.remove_script_unconfident:
                df = df.filter(df.is_script_unconfident == False)
            if self.config.remove_sparse_blocked:
                df = df.filter(df.contains_sparse_blocks == False)

        df = self.salting(df, self.n_splits)
        df.write.mode("overwrite").parquet(output_path)

    def run_data_parallelized(
        self,
        spark,
        df,
        docs_per_partition,
        perform_flagging,
        perform_removal,
        output_path,
    ):
        raise NotImplementedError(f"`run_data_parallelized` function has not been implemented for class `{self.name}`")

    def run_spark(
        self,
        spark,
        pf_parquets_path,
        pf_run_mode,
        pf_samples_per_partition,
        pf_perform_flagging,
        pf_perform_removal,
        pf_output_path,
        run_local,
    ):

        df = spark.read.format("parquet").load(pf_parquets_path)

        if pf_run_mode == "stage":
            return self.run_stage_parallelized(
                df=df,
                docs_per_partition=pf_samples_per_partition,
                perform_flagging=pf_perform_flagging,
                perform_removal=pf_perform_removal,
                output_path=pf_output_path,
            )
        else:
            raise Exception("Incorrect input for `pf_run_mode`. `pf_run_mode` only supports 1 types: `stage`.")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs) 

    def run_normal(
        self,
        pf_parquets_path,
        pf_run_mode,
        pf_samples_per_partition,
        pf_perform_flagging,
        pf_perform_removal,
        pf_output_path,
        run_local,
    ):
        raise NotImplementedError(f"`run_normal` function has not been implemented for class `{self.name}`")