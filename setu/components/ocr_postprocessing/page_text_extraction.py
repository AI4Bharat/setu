import argparse
from core import SetuStage, str2bool
import subprocess
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from .ocr_filters import parse_ocr_output

parse_ocr_output_udf = udf(parse_ocr_output, StringType())

class PageTextExtractionStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--pte_parquets_path",
            type=str,
            help="Path of parquets for page analysis in OCR postprocessing"
        )

        parser.add_argument(
            "--pte_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--pte_run_mode",
            type=str,
            required=False,
            choices=["stage"],
            default="stage",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--pte_output_path",
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
        df = df.select("*", parse_ocr_output_udf("blocks", "bbox_to_keep").alias("text"))
        df = df.drop("blocks")
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
        pte_parquets_path,
        pte_run_mode,
        pte_samples_per_partition,
        pte_output_path,
        run_local,
    ):

        df = spark.read.format("parquet").load(pte_parquets_path)

        if pte_run_mode == "stage":
            return self.run_stage_parallelized(
                df=df,
                docs_per_partition=pte_samples_per_partition,
                output_path=pte_output_path,
            )
        else:
            raise Exception("Incorrect input for `pte_run_mode`. `pte_run_mode` only supports 1 types: `stage`.")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs) 

    def run_normal(
        self,
        pte_parquets_path,
        pte_run_mode,
        pte_samples_per_partition,
        pte_output_path,
        run_local,
    ):
        raise NotImplementedError(f"`run_normal` function has not been implemented for class `{self.name}`")