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
from .document_filters import (
    split_with_delimiter,
    get_char_ngram_repetition,
    get_word_ngram_repetition,
    has_repetition,
    extract_document_metadata,
    normalize_text,
)
from .line_filters import (
    get_stop_word_dist,
    get_nsfw_words_pos,
    get_nsfw_word_dist,
    non_li_chars_total_count,
    get_word_count,
    get_char_count,
    get_bytes,
    get_nsfw_words_total_count,
    is_numbers,
)

split_with_delimiter_udf = udf(split_with_delimiter, ArrayType(StringType()))
get_nsfw_word_dist_udf = udf(get_nsfw_word_dist, MapType(StringType(), IntegerType()))
non_li_chars_total_count_udf = udf(non_li_chars_total_count, IntegerType())
get_word_count_udf = udf(get_word_count, IntegerType())
get_char_count_udf = udf(get_char_count, IntegerType())
get_bytes_udf = udf(get_bytes, IntegerType())
get_nsfw_words_total_count_udf = udf(get_nsfw_words_total_count, IntegerType())
is_numbers_udf = udf(is_numbers, BooleanType()) # Line Level
normalize_text_udf = udf(
    partial(normalize_text, 
            remove_nuktas=False,
            nasals_mode='do_nothing',
            do_normalize_chandras=False,
            do_normalize_vowel_ending=False
    ), 
    StringType()
)

class AnalysisStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)
        
        if self.spark_present:
            self.wrap_funcs_to_udf()
            self.spark_optimized_handler = SparkOptimizedHandlers()
            self.chunk_handler = ChunkHandler()
    
    def wrap_funcs_to_udf(self):
        self.get_char_ngram_repetition_udf = udf(
            partial(
                get_char_ngram_repetition,
                ngrams_arr=tuple(map(int, list(self.config.char_ngram_cum_thresholds.keys()))),
                for_spark=True,
            ), 
            MapType(StringType(), FloatType())
        )
        self.get_word_ngram_repetition_udf = udf(
            partial(
                get_word_ngram_repetition,
                ngrams_arr=tuple(map(int, list(self.config.word_ngram_cum_thresholds.keys()))),
                for_spark=True,
            ),
            MapType(StringType(), FloatType())
        )

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--analysis_df_parquets_path",
            type=str,
            help="Path to folder containing parquets",
        )

        parser.add_argument(
            "--is_analysis_df_path_batched",
            type=str2bool,
            required=False,
            default=True,
            help="Is path a batch path or not?",
        )

        parser.add_argument(
            "--analysis_additional_cols_to_use",
            type=list_of_strings,
            help="`,` separated additional columns to select from analysis parquets",
        )

        parser.add_argument(
            "--analysis_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--analysis_verbose",
            type=str2bool,
            default=False,
            required=False,
            help="Whether to add `show()`",
        )

        parser.add_argument(
            "--analysis_run_mode",
            type=str,
            required=False,
            choices=["stage"],
            default="stage",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--line_stats_output_path",
            type=str,
            help="Path of the folder store line stats checkpoint",
        )

        parser.add_argument(
            "--doc_stats_output_path",
            type=str,
            help="Path of the folder store doc stats checkpoint",
        )

        parser.add_argument(
            "--analysis_output_path",
            type=str,
            help="Path to the folder to store analysis output",
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

    def line_stats_collection(self, line_df, text_col, line_stats_output_path, verbose):

        line_df = line_df.withColumn(text_col, normalize_text_udf(text_col, "doc_lang"))

        line_df = line_df.select("*", is_numbers_udf(text_col, "doc_lang").alias("is_number"))

        if verbose:
            line_df.show(n=5)
            print("Completed `is_number`....")

        if self.config.remove_only_number:
            line_df = line_df.filter(line_df.is_number == False)
            
            if verbose:
                line_df.show(n=5)
                print("Completed `is_number` removal filter....")

        line_df = line_df.select("*", get_word_count_udf(text_col).alias("words_count"))

        if verbose:
            line_df.show(n=5)
            print("Completed `words_count`....")
        

        line_df = line_df.select("*", get_char_count_udf(text_col).alias("char_count"))

        if verbose:
            line_df.show(n=5)
            print("Completed `char_count`....")
        

        line_df = line_df.select("*", get_bytes_udf(text_col).alias("bytes"))

        if verbose:
            line_df.show(n=5)
            print("Completed `bytes`....")
        
        line_df = line_df.select("*", get_nsfw_word_dist_udf(text_col, "doc_lang").alias("nsfw_word_dist"))

        if verbose:
            line_df.show(n=5)
            print("Completed `nsfw_word_dist`....")
        

        line_df = line_df.select("*", get_nsfw_words_total_count_udf("nsfw_word_dist").alias("nsfw_words_count"))

        if verbose:
            line_df.show(n=5)
            print("Completed `nsfw_words_count`....")

        line_df = line_df.select("*", non_li_chars_total_count_udf(text_col).alias("non_li_char_count"))

        if verbose:
            line_df.show(n=5)
            print("Completed `non_li_char_count`....")

        line_df.write.mode("overwrite") \
                .parquet(line_stats_output_path)

        print(f"Completed line-level `df` parquet write.... to: {line_stats_output_path}")

        return line_df
        
    def aggregate_to_doc_stats(self, line_df, doc_id_col, text_col, drop_repeated_line_dist,  verbose):

        doc_stats_df = self.spark_optimized_handler.run_analysis(
            line_df=line_df,
            doc_id_col=doc_id_col,
            # text_col=text_col,
            line_nsfw_count_col_="nsfw_words_count",
            line_non_li_count_col_="non_li_char_count",
            line_bytes_col_="bytes",
            line_words_count_col_="words_count",
            line_char_count_col_="char_count",
        )

        if drop_repeated_line_dist:
            doc_stats_df = doc_stats_df.drop("repeated_line_dist")

        doc_stats_df.cache()

        if verbose:
            doc_stats_df.show(n=5)
            print("Completed `line2doc` run with metadata/stats aggregation....")

        return doc_stats_df

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

    def collect_repetition_scores(self, df, doc_stats_df, doc_id_col, text_col, verbose):

        char_ngram_score = df.select(doc_id_col, self.get_char_ngram_repetition_udf(text_col).alias("char_ngram_repetition_score"))
        char_ngram_score = char_ngram_score.select("*", *[char_ngram_score.char_ngram_repetition_score[f"{i}_gram_characters_repetition_score"].alias(f"{i}_gram_characters_repetition_score") 
                                                                                    for i in self.config.char_ngram_cum_thresholds.keys()])        

        if verbose:
            df.show(n=5)
            print("Completed `char_ngram_reptition_score`....")

        word_ngram_score = df.select(doc_id_col, self.get_word_ngram_repetition_udf(text_col, "doc_lang_iso").alias("word_ngram_repetition_score"))
        word_ngram_score = word_ngram_score.select("*", *[word_ngram_score.word_ngram_repetition_score[f"{i}_gram_words_repetition_score"].alias(f"{i}_gram_words_repetition_score") 
                                                                                    for i in self.config.word_ngram_cum_thresholds.keys()])

        if verbose:
            df.show(n=5)
            print("Completed `word_ngram_reptition_score`....")

        doc_stats_df = doc_stats_df \
                            .join(char_ngram_score, [doc_id_col]) \
                            .join(word_ngram_score, [doc_id_col])

        return doc_stats_df

    def run_stage_parallelized(
        self,
        df,
        additional_cols_to_use,
        doc_id_col,
        text_col,
        docs_per_partition,
        line_stats_output_path,
        doc_stats_output_path,
        analysis_output_path,
        verbose:bool = True,
    ):

        print("Starting SETU Analysis Spark Pipeline...........")

        df = df.select(doc_id_col, text_col, *additional_cols_to_use)

        df = self.set_split_count_and_salt(df, docs_per_partition)

        line_df = self.convert_to_line(df, text_col, verbose)
        line_df = self.line_stats_collection(line_df, text_col, line_stats_output_path, verbose)
        doc_stats_df = self.aggregate_to_doc_stats(line_df, doc_id_col, text_col, True,  verbose)
        df = self.convert_to_doc(df, line_df, text_col, doc_id_col, verbose)
        
        if self.config.calculate_repetition_scores:
            doc_stats_df = self.collect_repetition_scores(df, doc_stats_df, doc_id_col, text_col, verbose)

        doc_stats_df.drop(text_col) \
                    .join(df.select(doc_id_col, "doc_lang"), [doc_id_col]) \
                    .withColumn("doc_lang_partition", col("doc_lang")) \
                    .write.partitionBy("doc_lang_partition") \
                    .mode("overwrite") \
                    .parquet(doc_stats_output_path)

        # rename_partitioned_directories(doc_stats_output_path, "doc_lang_partition")

        if verbose:
            doc_stats_df.show(n=5)

        print(f"Completed doc-level `doc_stats_df` parquet write.... to: {doc_stats_output_path}")

        df.withColumn("doc_lang_partition", col("doc_lang")) \
            .write.partitionBy("doc_lang_partition").mode("overwrite") \
            .parquet(analysis_output_path)

        # rename_partitioned_directories(analysis_output_path, "doc_lang_partition")

        print(f"Completed analysis `df` parquet write.... to: {analysis_output_path}")

        return analysis_output_path

    def run_data_parallelized(
        self,
        spark,
        df,
        additional_cols_to_use,
        doc_id_col,
        text_col,
        docs_per_partition,
        line_stats_output_path,
        doc_stats_output_path,
        analysis_output_path,
        verbose:bool = True,
    ):
        raise NotImplementedError("`run_data_parallelized` function has not been implemented for class `AnalysisStage`")

    def run_spark(
        self,
        spark,
        analysis_df_parquets_path,
        is_analysis_df_path_batched,
        analysis_additional_cols_to_use,
        analysis_samples_per_partition,
        analysis_verbose,
        analysis_run_mode,
        line_stats_output_path,
        doc_stats_output_path,
        analysis_output_path,
        run_local,
    ):

        if is_analysis_df_path_batched:
            if not run_local:
                subprocess.run([[
                    "gsutil",
                    "cp",
                    analysis_df_parquets_path,
                    "/tmp/analysis_batch.info"
                ]])
            with open("/tmp/analysis_batch.info", "r") as batch_f:
                parquet_list = [line.strip() for line in batch_f.readlines()]
            analysis_df = spark.read.format("parquet").load(parquet_list)
        else:
            analysis_df = spark.read.format("parquet").load(analysis_df_parquets_path)

        if analysis_run_mode == "stage":
            return self.run_stage_parallelized(
                df=analysis_df,
                additional_cols_to_use=analysis_additional_cols_to_use,
                doc_id_col="doc_id",
                text_col="text",
                docs_per_partition=analysis_samples_per_partition,
                line_stats_output_path=line_stats_output_path,
                doc_stats_output_path=doc_stats_output_path,
                analysis_output_path=analysis_output_path,
                verbose=analysis_verbose,
            )
        elif analysis_run_mode == "data":
            return self.run_data_parallelized(
                spark=spark,
                df=analysis_df,
                additional_cols_to_use=analysis_additional_cols_to_use,
                doc_id_col="doc_id",
                text_col="text",
                docs_per_partition=analysis_samples_per_partition,
                line_stats_output_path=line_stats_output_path,
                doc_stats_output_path=doc_stats_output_path,
                analysis_output_path=analysis_output_path,
                verbose=analysis_verbose,
            )
        else:
            raise Exception("Incorrect input for `analysis_run_mode`. `analysis_run_mode` only supports 2 types: `stage` & `data`.")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs)

    def run_normal(
        self,
        analysis_df_parquets_path,
        is_analysis_df_path_batched,
        analysis_additional_cols_to_use,
        analysis_samples_per_partition,
        analysis_verbose,
        analysis_run_mode,
        line_stats_output_path,
        doc_stats_output_path,
        analysis_output_path,
        run_local,
    ):
        raise NotImplementedError("`run_normal` function has not been implemented for class `AnalysisStage`")