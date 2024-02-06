import argparse
from core import SetuStage, str2bool
import subprocess
from pyspark.sql.functions import col
import glob
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    Row,
    IntegerType,
    ArrayType,
    BooleanType,
    StringType,
    FloatType,
    StructType,
    StructField,
)
from .ocr_filters import (
    approx_intersection_cleaner,
    check_script_coverage,
    check_plane_coverage,
    check_bbox_overlap,
    check_block_density,
)
from functools import partial

approx_intersection_cleaner_udf = udf(
    partial(approx_intersection_cleaner, for_spark=True),
    StructType([
        StructField("bbox_to_keep", ArrayType(IntegerType()), True),
        StructField("bbox_to_keep_count", IntegerType(), True),
        StructField("total_bbox_count", IntegerType(), True)
    ]),
)
check_horizontal_coverage_udf = udf(
    partial(check_plane_coverage, type="horizontal"),
    FloatType(),
)
check_vertical_coverage_udf = udf(
    partial(check_plane_coverage, type="horizontal"),
    FloatType(),
)
check_bbox_overlap_udf = udf(check_bbox_overlap, FloatType())
check_script_coverage_udf = udf(
    partial(check_script_coverage, to_fail_threshold=0.5, failed_paras_ratio=0.3),
    BooleanType(),
)
check_block_density_udf = udf(check_block_density, FloatType())

class PageAnalysisStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--pa_parquets_path",
            type=str,
            help="Path of parquets for page analysis in OCR postprocessing"
        )

        parser.add_argument(
            "--pa_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--pa_run_mode",
            type=str,
            required=False,
            choices=["stage"],
            default="stage",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--pa_output_path",
            type=str,
            help="Directory where all the analysised pages will be stored."
        )
        
        return parser

    def run_stage_parallelized(
        self,
        df,
        docs_per_partition,
        output_path,
    ):
        df = self.set_split_count_and_salt(df, docs_per_partition)
        before_cols = df.columns
        df = df \
            .select("*", approx_intersection_cleaner_udf("blocks").alias("intersection_cleaner")) \
            .select(*before_cols, "intersection_cleaner.*") \
            .select("*", check_horizontal_coverage_udf("blocks", "bbox_to_keep").alias("horizontal_coverage")) \
            .select("*", check_vertical_coverage_udf("blocks", "bbox_to_keep").alias("vertical_coverage")) \
            .select("*", check_bbox_overlap_udf("blocks", "bbox_to_keep").alias("max_iou")) \
            .select("*", check_script_coverage_udf("blocks").alias("script_coverage")) \
            .select("*", check_block_density_udf("blocks", "bbox_to_keep").alias("min_block_density"))
        df = self.salting(df, self.n_splits)
        df.write.mode("overwrite").parquet(output_path)
        
    def run_data_parallelized(
        self,
        spark,
        df,
        docs_per_partition,
        output_path,
    ):
        raise NotImplementedError(f"`run_data_parallelized` function has not been implemented for class `{self.name}`")

    def run_spark(
        self,
        spark,
        pa_parquets_path,
        pa_run_mode,
        pa_samples_per_partition,
        pa_output_path,
        run_local
    ):

        df = spark.read.format("parquet").load(pa_parquets_path)

        if pa_run_mode == "stage":
            return self.run_stage_parallelized(
                df=df,
                docs_per_partition=pa_samples_per_partition,
                output_path=pa_output_path,
            )
        else:
            raise Exception("Incorrect input for `pa_run_mode`. `pa_run_mode` only supports 1 types: `stage`")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs) 

    def run_normal(
        self,
        pa_parquets_path,
        pa_run_mode,
        pa_samples_per_partition,
        pa_output_path,
        run_local
    ):
        raise NotImplementedError(f"`run_normal` function has not been implemented for class `{self.name}`")