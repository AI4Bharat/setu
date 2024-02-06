import argparse
from core import SetuStage, str2bool
import subprocess
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import time
from pyspark.sql.functions import sha2, concat_ws
from .ocr_filters import get_ocr_url

get_ocr_url_udf = F.udf(get_ocr_url, StringType())

class PageMergeStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--pm_parquets_path",
            type=str,
            help="Path of parquets for page analysis in OCR postprocessing"
        )

        parser.add_argument(
            "--pm_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--pm_run_mode",
            type=str,
            required=False,
            choices=["stage"],
            default="stage",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--pm_output_path",
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
        
        # Window specification
        w = Window().partitionBy("uri").orderBy("pageNumber")

        # Calculate difference between current and previous page_no
        df = df.withColumn("diff", F.col("pageNumber") - F.lag("pageNumber", 1).over(w))

        # Create group id for consecutive pages
        df = df.withColumn("group_id", F.sum(F.when(F.col("diff") > 1, 1).otherwise(0)).over(w))

        # Group by pdf_name and group_id, and aggregate the text
        df = df.groupBy("uri", "group_id").agg(
            F.concat_ws("", F.collect_list("text")).alias("text"), 
            F.concat_ws(",", F.collect_list("pageNumber")).alias("page_nos"), 
            F.count("pageNumber").alias("pages_count")
        ).orderBy("uri", "group_id")

        df = self.salting(df, self.n_splits)

        df = df.select(
            F.sha2(F.concat_ws("||", *df.columns), 256).alias("doc_id"), 
            "*").drop(*["group_id"])

        df = df.withColumn("url", get_ocr_url_udf("uri", "page_nos"))

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
        pm_parquets_path,
        pm_run_mode,
        pm_samples_per_partition,
        pm_output_path,
        run_local,
    ):

        df = spark.read.format("parquet").load(pm_parquets_path)

        if pm_run_mode == "stage":
            return self.run_stage_parallelized(
                df=df,
                docs_per_partition=pm_samples_per_partition,
                output_path=pm_output_path,
            )
        else:
            raise Exception("Incorrect input for `pm_run_mode`. `pm_run_mode` only supports 1 types: `stage`")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs) 

    def run_normal(
        self,
        pm_parquets_path,
        pm_run_mode,
        pm_samples_per_partition,
        pm_output_path,
        run_local,
    ):
        raise NotImplementedError(f"`run_normal` function has not been implemented for class `{self.name}`")