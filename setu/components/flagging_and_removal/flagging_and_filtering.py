import argparse
from core import (
    SetuStage,
    ChunkHandler, 
    SparkOptimizedHandlers, 
    rename_partitioned_directories,
    str2bool
)
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf
from components.crawl_analysis import has_repetition
from functools import partial
import subprocess

class FlaggingAndFilteringStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

        if self.spark_present:
            self.spark_optimized_handler = SparkOptimizedHandlers()
            self.wrap_funcs_with_udf()

    def wrap_funcs_with_udf(self):
        self.has_char_repetition_udf = udf(
            partial(
                has_repetition, 
                repetition_thresholds=self.config.char_ngram_cum_thresholds
            ),
            BooleanType()
        ) # Doc level
        self.has_word_repetition_udf = udf(
            partial(
                has_repetition, 
                repetition_thresholds=self.config.word_ngram_cum_thresholds
            ),  
            BooleanType()
        ) # Doc level

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--doc_stats_parquets_path",
            type=str,
            help="Path to folder containing parquets",
        )

        parser.add_argument(
            "--is_doc_stats_path_batched",
            type=str2bool,
            required=False,
            default=True,
            help="Is path a batch path or not?",
        )

        parser.add_argument(
            "--fnf_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--fnf_verbose",
            type=str2bool,
            default=False,
            required=False,
            help="Whether to add `show()`",
        )

        parser.add_argument(
            "--fnf_run_mode",
            type=str,
            required=False,
            choices=["stage"],
            default="stage",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--save_nsfw_data",
            type=str2bool,
            default=True,
            required=False,
            help="Whether to store nsfw data",
        )

        parser.add_argument(
            "--nsfw_output_path",
            type=str,
            help="Path of the folder to store nsfw data",
        )

        parser.add_argument(
            "--filtered_doc_stats_output_path",
            type=str,
            help="Path to the folder to store fitered output",
        )

        return parser

    def run_stage_parallelized(
        self,
        doc_stats_df,
        docs_per_partition,
        save_nsfw_data,
        nsfw_output_path,
        filtered_doc_stats_output_path,
        verbose:bool = True,
    ):

        print("Starting SETU Flagging & Filtering Spark Pipeline...........")

        if not doc_stats_df:
            raise Exception("Need to pass both `doc_stats_df` and `df` when `run_flagging` is `True`...")

        doc_stats_df = self.set_split_count_and_salt(doc_stats_df, docs_per_partition)

        doc_stats_df = self.spark_optimized_handler.run_flagging(
            doc_df=doc_stats_df,
            word_count_col="words_count",
            char_count_col="char_count",
            nsfw_count_col="nsfw_words_count",
            nsfw_threshold=self.config.nsfw_threshold,
            non_li_count_col="non_li_char_count",
            non_li_threshold=self.config.non_li_char_threshold,
            line_count_col="lines_count",
            min_line_count=self.config.min_line_count,
            mean_line_len_col="mean_line_length",
            min_mean_line_len=self.config.min_mean_line_len,
        )

        doc_stats_df = self.salting(doc_stats_df, self.n_splits)

        if verbose:
            doc_stats_df.show(n=5)
            print("Completed `doc_flagging`....")

        if self.config.char_repetition_filter:
            doc_stats_df = doc_stats_df.select("*", self.has_char_repetition_udf("char_ngram_repetition_score").alias("has_char_repetition")) \
                                        .drop("char_ngram_repetition_score")

        if self.config.word_repetition_filter:
            doc_stats_df = doc_stats_df.select("*", self.has_word_repetition_udf("word_ngram_repetition_score").alias("has_word_repetition")) \
                                        .drop("word_ngram_repetition_score")

        if verbose:
            doc_stats_df.show(n=5)
            print("Completed `has_char_reptition` & `has_word_reptition`....")

        if self.config.line_count_filter:

            doc_stats_df = doc_stats_df.filter(doc_stats_df.has_less_lines == False)

            if verbose:
                doc_stats_df.show(n=5)
                print("Completed `has_less_lines` removal filter....")

        if self.config.line_length_filter:

            doc_stats_df = doc_stats_df.filter(doc_stats_df.is_short_lines_heavy == False)

            if verbose:
                doc_stats_df.show(n=5)
                print("Completed `is_short_lines_heavy` removal filter....")

        if self.config.nsfw_filter:

            if self.config.save_nsfw_data:
                nsfw_stats_df = doc_stats_df.filter(doc_stats_df.is_nsfw_heavy == True)
                nsfw_stats_df.write.mode("overwrite") \
                                .parquet(nsfw_output_path if nsfw_output_path else self.config.nsfw_output_path)
                nsfw_stats_df.show(5)
                print("Completed nsfw `df` parquet write....")

            doc_stats_df = doc_stats_df.filter(doc_stats_df.is_nsfw_heavy == False)
            
            if verbose:
                doc_stats_df.show(n=5)               

        if self.config.non_li_filter:

            doc_stats_df = doc_stats_df.filter(doc_stats_df.is_non_li_heavy == False)

            if verbose:
                doc_stats_df.show(n=5)
                print("Completed `is_non_li_heavy` removal filter....")

        if self.config.word_repetition_filter:

            doc_stats_df = doc_stats_df.filter(doc_stats_df.has_word_repetition == False)

            if verbose:
                doc_stats_df.show(n=5)
                print("Completed `has_word_repetition` removal filter....")
            
        if self.config.char_repetition_filter:
            
            doc_stats_df = doc_stats_df.filter(doc_stats_df.has_char_repetition == False)

            if verbose:
                doc_stats_df.show(n=5)
                print("Completed `has_char_repetition` removal filter....")

        if verbose:
            doc_stats_df.show(n=5)
            print("Completed doc-level `df` parquet write....")

        doc_stats_df = self.salting(doc_stats_df, self.n_splits)

        doc_stats_df.write.mode("overwrite") \
                    .parquet(filtered_doc_stats_output_path)

        print(f"Completed filtered `doc_stats_df` parquet write...written to: {filtered_doc_stats_output_path}")


    def run_data_parallelized(
        self, 
        doc_stats_df,
        docs_per_partition,
        save_nsfw_data,
        nsfw_output_path,
        filtered_doc_stats_output_path,
        verbose:bool = True,
    ):
        raise NotImplementedError("`run_data_parallelized` function has not been implemented for class `FlaggingAndFilteringStage`")

    def run_spark(
        self,
        spark,
        doc_stats_parquets_path,
        is_doc_stats_path_batched,
        fnf_samples_per_partition,
        fnf_verbose,
        fnf_run_mode,
        save_nsfw_data,
        nsfw_output_path,
        filtered_doc_stats_output_path,
        run_local,
    ):

        if is_doc_stats_path_batched:
            if not run_local:
                subprocess.run([[
                    "gsutil",
                    "cp",
                    doc_stats_parquets_path,
                    "/tmp/fnf_batch.info"
                ]])
            with open("/tmp/fnf_batch.info", "r") as batch_f:
                parquet_list = [line.strip() for line in batch_f.readlines()]
            doc_stats_df = spark.read.format("parquet").load(parquet_list)
        else:
            doc_stats_df = spark.read.format("parquet").load(doc_stats_parquets_path)

        doc_stats_df = doc_stats_df.dropDuplicates(["doc_id"])

        if fnf_run_mode == "stage":
            return self.run_stage_parallelized(
                doc_stats_df=doc_stats_df,
                docs_per_partition=fnf_samples_per_partition,
                save_nsfw_data=save_nsfw_data,
                nsfw_output_path=nsfw_output_path,
                filtered_doc_stats_output_path=filtered_doc_stats_output_path,
                verbose=fnf_verbose,
            )
        elif fnf_run_mode == "data":
            return self.run_data_parallelized(
                spark=spark,
                doc_stats_df=doc_stats_df,
                docs_per_partition=fnf_samples_per_partition,
                save_nsfw_data=save_nsfw_data,
                nsfw_output_path=nsfw_output_path,
                filtered_doc_stats_output_path=filtered_doc_stats_output_path,
                verbose=fnf_verbose,
            )
        else:
            raise Exception("Incorrect input for `fnf_run_mode`. `fnf_run_mode` only supports 2 types: `stage` & `data`.")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs) 

    def run_normal(
        self, 
        doc_stats_parquets_path,
        is_doc_stats_path_batched,
        fnf_samples_per_partition,
        fnf_verbose,
        fnf_run_mode,
        save_nsfw_data,
        nsfw_output_path,
        filtered_doc_stats_output_path,
        run_local
    ):
        raise NotImplementedError("`run_normal` function has not been implemented for class `FlaggingAndFilteringStage`")