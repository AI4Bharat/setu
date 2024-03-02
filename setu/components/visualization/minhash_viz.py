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

class VisualizeMinhashStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

        if not self.spark_present:
            raise Exception(f"Currently, {self.name} can only run in spark environment")

        
    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--minhash_parquets",
            type=str,
            help="Path to folder containing parquets",
        )

        parser.add_argument(
            "--minhash_viz_output",
            type=str,
            help="Path to folder where viz output will be stored",
        )

        parser.add_argument(
            "--min_viz_gcp_bucket",
            type=str,
            default=None,
            required=False,
            help="Bucket to store data in if not running in locally",
        )

        parser.add_argument(
            "--viz_minhash_stage_run_mode",
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
        minhash_df,
        minhash_viz_output,
        min_viz_gcp_bucket,
        run_local
    ):
        
        
        
        raise NotImplementedError("`run_stage_parallelized` function has not been implemented for class `VisualizeMinhashStage`")

    def run_data_parallelized(
        self,
        spark,
        minhash_df,
        minhash_viz_output,
        min_viz_gcp_bucket,
        run_local
    ):
        raise NotImplementedError("`run_data_parallelized` function has not been implemented for class `VisualizeMinhashStage`")

    def run_spark(
        self,
        spark,
        minhash_parquets,
        minhash_viz_output,
        min_viz_gcp_bucket,
        viz_minhash_stage_run_mode,
        run_local,
    ):

        if not run_local:
            self.client = storage.Client()
            self.min_viz_gcp_bucket = self.client.bucket(min_viz_gcp_bucket)

        minhash_df = spark.read.format("parquet").options(inferSchema="true").parquet(minhash_parquets)

        if viz_minhash_stage_run_mode == "stage":
            return self.run_stage_parallelized(
                minhash_df=minhash_df,
                minhash_viz_output=minhash_viz_output,
                min_viz_gcp_bucket=min_viz_gcp_bucket,
                run_local=run_local,
            )
        elif viz_minhash_stage_run_mode == "data":
            return self.run_data_parallelized(
                spark=spark,
                minhash_df=minhash_df,
                minhash_viz_output=minhash_viz_output,
                min_viz_gcp_bucket=min_viz_gcp_bucket,
                run_local=run_local,
            )
        else:
            raise Exception("Incorrect input for `viz_minhash_stage_run_mode`. `viz_minhash_stage_run_mode` only supports 2 types: `stage` & `data`.")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs)

    def run_normal(
        self,
        minhash_parquets,
        minhash_viz_output,
        min_viz_gcp_bucket,
        min_analysis_stage_run_mode,
        run_local,
    ):
        raise NotImplementedError("`run_normal` function has not been implemented for class `VisualizeMinhashStage`") 