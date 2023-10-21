import argparse
import subprocess
from core import SetuStage, str2bool, list_of_strings
from pyspark.sql.functions import col
from .lid_core import (
    LIDPipeline,
    run_lid_on_each_partition_with_idx,
    run_lid_spark_pipeline,
)
import os

class LIDStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--lid_df_parquets_path",
            type=str,
            help="Path to folder containing parquets",
        )

        parser.add_argument(
            "--is_lid_df_path_batched",
            type=str2bool,
            required=False,
            default=True,
            help="Is path a batch path or not?",
        )

        parser.add_argument(
            "--lid_additional_cols",
            type=list_of_strings,
            help="`,` separated additional columns to select from parquets",
        )

        parser.add_argument(
            "--lid_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--lid_verbose",
            type=str2bool,
            default=False,
            required=False,
            help="Whether to add `show()`",
        )

        parser.add_argument(
            "--lid_run_mode",
            type=str,
            required=False,
            choices=["data"],
            default="data",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--doc_lid_output_path",
            type=str,
            help="Path of the folder store lid checkpoint",
        )
        
        return parser

    def run_stage_parallelized(
        self,
        df,
        doc_id_col,
        text_col,
        additional_cols_to_use,
        docs_per_partition,
        doc_lid_output_path,
        verbose: bool = True,
    ):
        raise NotImplementedError("`run_stage_parallelized` function has not been implemented for class `LIDStage`")
    
    def run_data_parallelized(
        self,
        spark,
        df,
        doc_id_col,
        text_col,
        additional_cols_to_use,
        docs_per_partition,
        doc_lid_output_path,
        verbose: bool = True,
    ):
        print("Starting SETU LID Segregation Spark Pipeline...........")

        print(f"Input document count: {df.count()}")

        df = df.na.drop(subset=[text_col])

        self.df_total_rows = df.count()

        print(f"Count after filtering for `None` values {text_col} col: {self.df_total_rows}")

        df = df.select(doc_id_col, text_col, *additional_cols_to_use)
        df = self.set_split_count_and_salt(df, docs_per_partition)
        df.cache()

        df = run_lid_spark_pipeline(spark, self.config, df, [doc_id_col] + additional_cols_to_use, text_col, "doc_lang", "doc_lang_iso", "/opt/setu/filter_data")
        
        if verbose:
            df.show(n=5)
            print("Completed `doc_lang`....")

        df = self.salting(df, self.n_splits)

        # Duplicate the doc_lang column as doc_lang_partition
        df = df.withColumn("doc_lang_partition", col("doc_lang"))

        df.write.partitionBy("doc_lang_partition").mode("overwrite") \
            .parquet(doc_lid_output_path)

        # rename_partitioned_directories(doc_lid_output_path, "doc_lang_partition")

        print(f"Completed `doc_lang` level `df` parquet write.... to: {doc_lid_output_path}")

    def run_spark(
        self,
        spark,
        lid_df_parquets_path,
        is_lid_df_path_batched,
        lid_additional_cols,
        lid_samples_per_partition,
        lid_verbose,
        lid_run_mode,
        doc_lid_output_path,
        run_local,
    ):

        if is_lid_df_path_batched:
            if not run_local:
                subprocess.run([[
                    "gsutil",
                    "cp",
                    lid_df_parquets_path,
                    "/tmp/lid_batch.info"
                ]])
            with open("/tmp/lid_batch.info", "r") as batch_f:
                parquet_list = [line.strip() for line in batch_f.readlines()]
            doc_df = spark.read.format("parquet").load(parquet_list)
        else:
            doc_df = spark.read.format("parquet").load(lid_df_parquets_path)

        if lid_run_mode == "stage":
            return self.run_stage_parallelized(
                df=doc_df,
                doc_id_col="doc_id",
                text_col="text",
                additional_cols_to_use=lid_additional_cols,
                docs_per_partition=lid_samples_per_partition,
                doc_lid_output_path=doc_lid_output_path,
                verbose=lid_verbose,
            )
        elif lid_run_mode == "data":
            return self.run_data_parallelized(
                spark=spark,
                df=doc_df,
                doc_id_col="doc_id",
                text_col="text",
                additional_cols_to_use=lid_additional_cols,
                docs_per_partition=lid_samples_per_partition,
                doc_lid_output_path=doc_lid_output_path,
                verbose=lid_verbose,
            )
        else:
            raise Exception("Incorrect input for `lid_run_mode`. `lid_run_mode` only supports 2 types: `stage` & `data`.")

    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs)

    def run_normal(
        self,
        lid_df_parquets_path,
        is_lid_df_path_batched,
        lid_additional_cols,
        lid_samples_per_partition,
        lid_verbose,
        lid_run_mode,
        doc_lid_output_path,
        run_local,
    ):
        raise NotImplementedError("`run_normal` function has not been implemented for class `LIDStage`")