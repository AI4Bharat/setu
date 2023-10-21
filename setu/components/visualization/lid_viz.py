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


class VisualizeLIDStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

        if not self.spark_present:
            raise Exception(f"Currently, {self.name} can only run in spark environment")

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--lid_parquets",
            type=str,
            help="Path to folder containing parquets",
        )

        parser.add_argument(
            "--cleaned_docs_parquets",
            type=str,
            help="Path to folder containing parquets",
        )

        parser.add_argument(
            "--lid_viz_output",
            type=str,
            help="Path to folder where viz output will be stored",
        )

        parser.add_argument(
            "--lid_viz_gcp_bucket",
            type=str,
            default=None,
            required=False,
            help="Bucket to store data in if not running in locally",
        )

        parser.add_argument(
            "--viz_lid_stage_run_mode",
            type=str,
            required=False,
            choices=["stage"],
            default="stage",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--viz_lid_perform_join",
            type=str2bool,
            default=True,
            required=False,
            help="Whether to perform join between lid_df and cleaned_docs_df using `doc_id` column",
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
        lid_df,
        lid_viz_output,
        run_local
    ):

        uncleaned_bytes_pd = lid_df.groupby("doc_lang").agg(sum("uncleaned_bytes")).withColumn("size(in GB)", in_gb("sum(uncleaned_bytes)")).drop("sum(uncleaned_bytes)").sort("doc_lang").toPandas()
        self.save_csv(
            df=uncleaned_bytes_pd, 
            base_dir=lid_viz_output, 
            filename="uncleaned_bytes_per_lang.csv", 
            run_local=run_local, 
            bucket=self.lid_viz_gcp_bucket
        )

        doc_count_pd = lid_df.groupby("doc_lang").count().sort("doc_lang").toPandas()
        self.save_csv(
            df=doc_count_pd, 
            base_dir=lid_viz_output, 
            filename="doc_count_per_lang.csv", 
            run_local=run_local, 
            bucket=self.lid_viz_gcp_bucket
        )

        uncleaned_words_count_pd = lid_df.groupby("doc_lang").agg(sum("uncleaned_words_count")).sort("doc_lang").toPandas()
        self.save_csv(
            df=uncleaned_words_count_pd, 
            base_dir=lid_viz_output, 
            filename="uncleaned_words_count_per_lang.csv", 
            run_local=run_local, 
            bucket=self.lid_viz_gcp_bucket
        )


    def run_data_parallelized(
        self,
        spark,
        lid_df,
        lid_viz_output,
        run_local,
    ):
        raise NotImplementedError("`run_data_parallelized` function has not been implemented for class `VisualizeLIDStage`")

    def run_spark(
        self,
        spark,
        lid_parquets,
        cleaned_docs_parquets,
        lid_viz_output,
        lid_viz_gcp_bucket,
        viz_lid_stage_run_mode,
        viz_lid_perform_join,
        run_local,
    ):

        if not run_local:
            self.client = storage.Client()
            self.lid_viz_gcp_bucket = self.client.bucket(lid_viz_gcp_bucket)

        lid_df = spark.read.format("parquet").options(inferSchema="true").load(lid_parquets)
        if viz_lid_perform_join:
            cleaned_docs_df = spark.read.format("parquet").options(inferSchema="true").load(cleaned_docs_parquets)
            lid_df = lid_df.join(cleaned_docs_df, ["doc_id"])

        if viz_lid_stage_run_mode == "stage":
            return self.run_stage_parallelized(
                lid_df=lid_df,
                lid_viz_output=lid_viz_output,
                run_local=run_local
            )
        elif viz_lid_stage_run_mode == "data":
            return self.run_data_parallelized(
                spark=spark,
                lid_df=lid_df,
                lid_viz_output=lid_viz_output,
                run_local=run_local            
            )
        else:
            raise Exception("Incorrect input for `viz_lid_stage_run_mode`. `viz_lid_stage_run_mode` only supports 2 types: `stage` & `data`.")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs)

    def run_normal(
        self,
        lid_parquets,
        cleaned_docs_parquets,
        lid_viz_output,
        lid_viz_gcp_bucket,
        viz_lid_stage_run_mode,
        viz_lid_perform_join,
        run_local,
    ):
        raise NotImplementedError("`run_normal` function has not been implemented for class `VisualizeLIDStage`")