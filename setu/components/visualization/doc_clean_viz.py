import argparse
import subprocess
from core import (
    SetuStage,
    str2bool
)
import matplotlib.pyplot as plt
from google.cloud import storage
import pandas as pd
from pyspark.sql.functions import sum, udf, col
from pyspark.sql.types import FloatType
import os

in_gb = udf(lambda x: x/(1024 * 1024 * 1024), FloatType())


class VisualizeDocCleanStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

        if not self.spark_present:
            raise Exception(f"Currently, {self.name} can only run in spark environment")

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--viz_cleaned_docs_parquets",
            type=str,
            help="Path to folder containing parquets",
        )
        
        parser.add_argument(
            "--viz_symbol_heavy_parquets",
            type=str,
            help="Path to folder containing parquets",
        )

        parser.add_argument(
            "--doc_clean_viz_output",
            type=str,
            help="Path to folder where viz output will be stored",
        )

        parser.add_argument(
            "--dc_viz_gcp_bucket",
            type=str,
            default=None,
            required=False,
            help="Bucket to store data in if not running in locally",
        )

        parser.add_argument(
            "--viz_dc_stage_run_mode",
            type=str,
            required=False,
            choices=["stage"],
            default="stage",
            help="Which mode to run the stage in.",
        )

        return parser

    def save_csv(self, df, base_dir, filename, run_local, bucket):
        if run_local:
            df.to_csv(os.path.join(base_dir, filename))
        else:
            df.to_csv(f"/tmp/{filename}")
            blob = bucket.blob(os.path.join(base_dir.replace(f"gs://{bucket.name}/", ""), filename))
            blob.upload_from_filename(f"/tmp/{filename}")

    def save_png(self, base_dir, filename, run_local, bucket):
        if run_local:
            plt.savefig(os.path.join(base_dir, filename))
        else:
            plt.savefig(f"/tmp/{filename}")
            blob = bucket.blob(os.path.join(base_dir.replace(f"gs://{bucket.name}/", ""), filename))
            blob.upload_from_filename(f"/tmp/{filename}")

    def run_stage_parallelized(
        self,
        spark,
        cleaned_docs_df,
        symbol_heavy_df,
        doc_clean_viz_output,
        run_local
    ):
        cleaned_docs_count = cleaned_docs_df.count()
        null_cleaned_docs_count = cleaned_docs_df.filter(cleaned_docs_df.text.isNull()).count()
        symbol_heavy_count = symbol_heavy_df.count()
        cleaned_docs_count_pd = pd.DataFrame({
            "symbol_heavy_doc_count": [symbol_heavy_count],
            "null_cleaned_docs_count": [null_cleaned_docs_count],
            "cleaned_docs_count": [cleaned_docs_count],
            "total_docs": [symbol_heavy_count + cleaned_docs_count]
        })
        self.save_csv(
            df=cleaned_docs_count_pd, 
            base_dir=doc_clean_viz_output, 
            filename="cleaned_docs_count.csv", 
            run_local=run_local, 
            bucket=self.dc_viz_gcp_bucket
        )

        uncleaned_words_count_df = cleaned_docs_df.agg({'uncleaned_words_count': 'sum'}).toPandas()
        null_uncleaned_words_count_df = cleaned_docs_df.filter(cleaned_docs_df.text.isNull()).agg({'uncleaned_words_count': 'sum'}).toPandas()
        cleaned_docs_word_count_pd = pd.concat([uncleaned_words_count_df, null_uncleaned_words_count_df], axis=1)
        self.save_csv(
            df=cleaned_docs_word_count_pd, 
            base_dir=doc_clean_viz_output, 
            filename="cleaned_docs_word_count.csv", 
            run_local=run_local, 
            bucket=self.dc_viz_gcp_bucket
        )

        symbol_heavy_pd = symbol_heavy_df.withColumn(   
            "symbol_ratio", 
            symbol_heavy_df.symbol_ratio.cast('float')
        ).select("symbol_ratio").toPandas()
        self.save_csv(
            df=symbol_heavy_pd, 
            base_dir=doc_clean_viz_output, 
            filename="symbol_heavy.csv", 
            run_local=run_local, 
            bucket=self.dc_viz_gcp_bucket
        )

        symbol_heavy_pd.hist(bins=100)
        self.save_png(
            base_dir=doc_clean_viz_output, 
            filename="symbol_heavy_plot.png", 
            run_local=run_local, 
            bucket=self.dc_viz_gcp_bucket
        )

    def run_data_parallelized(
        self,
        spark,
        cleaned_docs_df,
        symbol_heavy_df,
        doc_clean_viz_output,
        run_local,
    ):
        raise NotImplementedError("`run_data_parallelized` function has not been implemented for class `VisualizeDocCleanStage`")

    def run_spark(
        self,
        spark,
        viz_cleaned_docs_parquets,
        viz_symbol_heavy_parquets,
        doc_clean_viz_output,
        dc_viz_gcp_bucket,
        viz_dc_stage_run_mode,
        run_local,
    ):

        if not run_local:
            self.client = storage.Client()
            self.dc_viz_gcp_bucket = self.client.bucket(dc_viz_gcp_bucket)

        cleaned_docs_df = spark.read.format("parquet").options(inferSchema="true").load(viz_cleaned_docs_parquets)
        symbol_heavy_df = spark.read.format("parquet").options(inferSchema="true").load(viz_symbol_heavy_parquets)

        if viz_dc_stage_run_mode == "stage":
            return self.run_stage_parallelized(
                spark=spark,
                cleaned_docs_df=cleaned_docs_df,
                symbol_heavy_df=symbol_heavy_df,
                doc_clean_viz_output=doc_clean_viz_output,
                run_local=run_local,
            )
        elif viz_dc_stage_run_mode == "data":
            return self.run_data_parallelized(
                spark=spark,
                cleaned_docs_df=cleaned_docs_df,
                symbol_heavy_df=symbol_heavy_df,
                doc_clean_viz_output=doc_clean_viz_output,
                run_local=run_local,
            )
        else:
            raise Exception("Incorrect input for `viz_dc_stage_run_mode`. `viz_dc_stage_run_mode` only supports 2 types: `stage` & `data`.")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs)

    def run_normal(
        self,
        viz_cleaned_docs_parquets,
        viz_symbol_heavy_parquets,
        doc_clean_viz_output,
        dc_viz_gcp_bucket,
        viz_dc_stage_run_mode,
        run_local,
    ):
        raise NotImplementedError("`run_normal` function has not been implemented for class `VisualizeDocCleanStage`")