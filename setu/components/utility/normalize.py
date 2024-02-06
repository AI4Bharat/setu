import argparse
import subprocess
from core import (
    SetuStage,
    ChunkHandler, 
    SparkOptimizedHandlers, 
    rename_partitioned_directories,
    str2bool,
    list_of_strings,
)
from pyspark.sql.functions import (
    udf,
    posexplode,
    col,
)
from pyspark.sql.types import (
    BooleanType,
    IntegerType, 
    ArrayType, 
    MapType, 
    StringType, 
    FloatType,
    StructType,
    StructField
)
from functools import partial
from components.crawl_analysis.document_filters import (
    split_with_delimiter,
    normalize_text,
)

split_with_delimiter_udf = udf(split_with_delimiter, ArrayType(StringType()))
normalize_text_udf = udf(
    partial(normalize_text, 
            remove_nuktas=False,
            nasals_mode='do_nothing',
            do_normalize_chandras=False,
            do_normalize_vowel_ending=False
    ), 
    StringType()
)

class NormalizeStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)
        
        if self.spark_present:
            self.spark_optimized_handler = SparkOptimizedHandlers()
            self.chunk_handler = ChunkHandler()

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--normalize_df_parquets_path",
            type=str,
            help="Path to folder containing parquets",
        )

        parser.add_argument(
            "--is_normalize_df_path_batched",
            type=str2bool,
            required=False,
            default=True,
            help="Is path a batch path or not?",
        )

        parser.add_argument(
            "--normalize_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--normalize_verbose",
            type=str2bool,
            default=False,
            required=False,
            help="Whether to add `show()`",
        )

        parser.add_argument(
            "--normalize_run_mode",
            type=str,
            required=False,
            choices=["stage"],
            default="stage",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--normalize_output_path",
            type=str,
            help="Path to the folder to store normalize output",
        )
        
        return parser

    def convert_to_line(self, df, text_col, verbose):

        line_df = df.withColumn(text_col, split_with_delimiter_udf(text_col))
        df.unpersist()
        
        if verbose:
            line_df.show(n=5)
            print("Completed `split_at_terminal` ....")

        line_df = line_df.select("*", posexplode(text_col)).drop(text_col).withColumnRenamed("col", text_col)

        line_df = self.salting(line_df, self.n_splits)
        line_df.cache()

        if verbose:
            line_df.show(n=5)
            print("Completed `posexplode` to get line-level `df` ....")

        return line_df

    def line_normalize(self, line_df, text_col):
        line_df = line_df.withColumn(text_col, normalize_text_udf(text_col, "doc_lang"))
        return line_df

    def convert_to_doc(self, df, line_df, text_col, doc_id_col, verbose):
        doc_df = self.chunk_handler.lines2doc(line_df, text_col, doc_id_col, "pos", "")
        doc_df = self.salting(doc_df, self.n_splits)

        doc_df.cache()
        
        if verbose:
            doc_df.show(n=5)
            print("Completed `lines2doc` via ` `....")
        
        df = df.drop(text_col) \
                .join(doc_df, [doc_id_col])

        df.cache()
        doc_df.unpersist()  
        
        if verbose:    
            df.show(n=5)
            print("Updated Text column with cleaned text....")

        return df

    def run_stage_parallelized(
        self,
        df,
        doc_id_col,
        text_col,
        docs_per_partition,
        normalize_output_path,
        verbose:bool = True,
    ):

        print("Starting SETU Normalize Spark Pipeline...........")

        df = self.set_split_count_and_salt(df, docs_per_partition)

        line_df = self.convert_to_line(df, text_col, verbose)
        line_df = self.line_normalize(line_df, text_col)
        df = self.convert_to_doc(df, line_df, text_col, doc_id_col, verbose)

        df.write.mode("overwrite") \
            .parquet(normalize_output_path)

        print(f"Completed normalize `df` parquet write.... to: {normalize_output_path}")

        return normalize_output_path

    def run_data_parallelized(
        self,
        spark,
        df,
        doc_id_col,
        text_col,
        docs_per_partition,
        normalize_output_path,
        verbose:bool = True,
    ):
        raise NotImplementedError(f"`run_data_parallelized` function has not been implemented for class `{self.name}`")

    def run_spark(
        self,
        spark,
        normalize_df_parquets_path,
        is_normalize_df_path_batched,
        normalize_samples_per_partition,
        normalize_verbose,
        normalize_run_mode,
        normalize_output_path,
        run_local,
    ):

        if is_normalize_df_path_batched:
            if not run_local:
                subprocess.run([[
                    "gsutil",
                    "cp",
                    normalize_df_parquets_path,
                    "/tmp/normalize_batch.info"
                ]])
            with open("/tmp/normalize_batch.info", "r") as batch_f:
                parquet_list = [line.strip() for line in batch_f.readlines()]
            normalize_df = spark.read.format("parquet").load(parquet_list)
        else:
            normalize_df = spark.read.format("parquet").load(normalize_df_parquets_path)

        if normalize_run_mode == "stage":
            return self.run_stage_parallelized(
                df=normalize_df,
                doc_id_col="doc_id",
                text_col="text",
                docs_per_partition=normalize_samples_per_partition,
                normalize_output_path=normalize_output_path,
                verbose=normalize_verbose,
            )
        else:
            raise Exception("Incorrect input for `normalize_run_mode`. `normalize_run_mode` only supports 1 types: `stage`.")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs)

    def run_normal(
        self,
        normalize_df_parquets_path,
        is_normalize_df_path_batched,
        normalize_samples_per_partition,
        normalize_verbose,
        normalize_run_mode,
        line_stats_output_path,
        doc_stats_output_path,
        normalize_output_path,
        run_local,
    ):
        raise NotImplementedError(f"`run_normal` function has not been implemented for class `{self.name}`")