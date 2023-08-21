from typing import Dict
import numpy as np
from pyspark.sql import SparkSession
from pyspark import TaskContext
from pyspark.sql.functions import (
    udf,
    posexplode,
    size,
    when,
    broadcast,
    rand,
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
import json
from constants import (
    CONSTANTS, 
    KW_PROCESSORS
)
from document_filters import (
    find_code_spans,
    has_code,
    remove_code,
    is_terminal_valid,
    remove_non_terminal_punc_span,
    terminal_punc_filter,
    split_at_terminal_punc,
    split_with_delimiter,
    get_char_ngram_repetition,
    get_word_ngram_repetition,
    has_repetition,
    extract_document_metadata,
    perform_doc_flagging,
)
from line_filters import (
    get_stop_word_dist,
    get_nsfw_words_pos,
    get_nsfw_word_dist,
    non_li_chars_total_count,
    get_word_count,
    get_char_count,
    get_bytes,
    get_nsfw_words_total_count,
    is_numbers,
    get_stopword_total_count,
    extract_line_metadata,
)
from lid import (
    LIDPipeline,
    run_lid_on_each_partition_with_idx,
    run_lid_spark_pipeline,
)
from utils import (
    ChunkHandler, 
    SparkOptimizedHandlers, 
    rename_partitioned_directories
)
from argparse import Namespace
import pyarrow as pa
import pyarrow.parquet as pq
import argparse
import os
from math import ceil
import time
import pandas as pd
import matplotlib.pyplot as plt

find_code_spans_udf = udf(
    find_code_spans, 
    StructType([StructField("code_spans",
                            ArrayType(ArrayType(IntegerType())), True),
                StructField("code_spans_success",
                            BooleanType(), True)]))
# find_code_spans_udf = udf(find_code_spans, ArrayType(ArrayType(IntegerType())))
is_terminal_valid_udf = udf(is_terminal_valid, BooleanType()) # Doc level
split_with_delimiter_udf = udf(split_with_delimiter, ArrayType(StringType()))
get_nsfw_word_dist_udf = udf(get_nsfw_word_dist, MapType(StringType(), IntegerType()))
non_li_chars_total_count_udf = udf(non_li_chars_total_count, IntegerType())
get_word_count_udf = udf(get_word_count, IntegerType())
get_char_count_udf = udf(get_char_count, IntegerType())
get_bytes_udf = udf(get_bytes, IntegerType())

class Setu():

    def __init__(self, config_file):
        
        self.config = self.load_config(config_file)

        if self.config.use_spark:
            self.wrap_funcs_to_udf()
            self.spark_optimized_handler = SparkOptimizedHandlers()
            self.chunk_handler = ChunkHandler()
        else:
            self.lid = LIDPipeline(**vars(self.config))
            self.constants = CONSTANTS
            self.kw_processors = KW_PROCESSORS
    
    def load_config(self, config_file):
        with open(config_file, "r") as config_file:
            config_= json.load(config_file)
        return Namespace(**config_)

    def wrap_funcs_to_udf(self):

        self.get_char_ngram_repetition = partial(
            get_char_ngram_repetition,
            ngrams_arr=tuple(map(int, list(self.config.char_ngram_cum_thresholds.keys()))),
            for_spark=True,
        )
        self.get_word_ngram_repetition = partial(
            get_word_ngram_repetition,
            ngrams_arr=tuple(map(int, list(self.config.word_ngram_cum_thresholds.keys()))),
            for_spark=True,
        )

        self.has_char_repetition = partial(has_repetition, repetition_thresholds=self.config.char_ngram_cum_thresholds)
        self.has_word_repetition = partial(has_repetition, repetition_thresholds=self.config.word_ngram_cum_thresholds)
        self.remove_code_udf = udf(remove_code, StringType())
        self.get_char_ngram_repetition_udf = udf(self.get_char_ngram_repetition, MapType(StringType(), FloatType()))
        self.get_word_ngram_repetition_udf = udf(self.get_word_ngram_repetition, MapType(StringType(), FloatType()))
        self.has_char_repetition_udf = udf(self.has_char_repetition, BooleanType()) # Doc level
        self.has_word_repetition_udf = udf(self.has_word_repetition, BooleanType()) # Doc level
        self.get_nsfw_words_total_count_udf = udf(get_nsfw_words_total_count, IntegerType())
        self.is_numbers_udf = udf(is_numbers, BooleanType()) # Line Level

    def salting(self, df, n_splits):
        # Number of salt buckets
        num_buckets = n_splits  # Adjust based on your data size and level of skewness

        # Adding a salt column to the DataFrame
        df = df.withColumn("salt", (rand() * num_buckets).cast("int"))

        # Repartition based on the salted key to ensure more even distribution
        df = df.repartition("salt")

        df = df.drop("salt")

        return df

    def set_split_count_and_salt(self, df, docs_per_partition):

        self.df_total_rows = df.count()

        self.n_splits = ceil(self.df_total_rows/docs_per_partition)

        print(f"When required data will be repartitioned into - {self.n_splits} partitions")

        df = self.salting(df, self.n_splits)

        return df

    def doc_clean_stage(self, df, cols_to_use, text_col, doc_id_col, verbose):

        curr_cols = list(df.schema.names)
        
        df = df.select("*", find_code_spans_udf(doc_id_col, text_col).alias("code_span_results")) \
                .select(*curr_cols, "code_span_results.*")
        # df = df.select("*", find_code_spans_udf(doc_id_col, text_col).alias("code_spans"))
        
        if verbose:
            df.explain(mode="formatted")
            df.show(n=5)
            print("Completed `find_code_spans`....")

        if self.config.remove_code:

            df = df.withColumn(text_col, self.remove_code_udf(text_col, "code_spans"))

            if verbose:
                df.explain(mode="formatted")
                df.show(n=5)
                print("Completed `remove_code`....")

        spans_df = self.chunk_handler.doc2lines(df, text_col, "\n")

        if verbose:
            spans_df.explain(mode="formatted")
            spans_df.show(n=5)
            print("Completed `doc2lines` via `\\n`....")

        spans_df = spans_df.select("*", is_terminal_valid_udf(text_col).alias("is_terminal_valid"))

        if verbose:
            spans_df.explain(mode="formatted")
            spans_df.show(n=5)
            print("Completed `is_terminal_valid`....")
        
        if self.config.remove_terminal_invalid:

            spans_df = spans_df.filter(spans_df.is_terminal_valid == True)

            if verbose:
                spans_df.explain(mode="formatted")
                spans_df.show(n=5)
                print("Completed `remove_terminal_invalid`....")

        doc_df = self.chunk_handler.lines2doc(spans_df, text_col, doc_id_col, "pos", '\n')
        spans_df.unpersist(True)
        doc_df = self.salting(doc_df, self.n_splits)

        if verbose:
            doc_df.show(n=5)
            print("Completed `lines2doc` via ` `....")

        df = df \
            .drop(text_col) \
            .join(doc_df, [doc_id_col])

        doc_df.unpersist(True)

        if verbose:
            df.show(n=5, truncate=False)

        return df

    def lid_stage(self, df, doc_id_col, text_col):
        df = self.salting(df, self.n_splits)
        df = run_lid_spark_pipeline(self.config, df, [doc_id_col], text_col, "doc_lang", "doc_lang_iso")
        df.cache()
        df.checkpoint()
        df.show(n=5)
        print("Completed `doc_lang`....")
        df.unpersist(True)
        return df

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
        line_df = line_df.select("*", self.is_numbers_udf(text_col, "doc_lang").alias("is_number"))

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
        

        line_df = line_df.select("*", self.get_nsfw_words_total_count_udf("nsfw_word_dist").alias("nsfw_words_count"))

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
            text_col=text_col,
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
        doc_df = self.chunk_handler.lines2doc(line_df, text_col, doc_id_col, "pos", " ")
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

    def run_lid_segregation_spark_pipeline(
        self,
        df,
        cols_to_use,
        doc_id_col,
        text_col,
        docs_per_partition,
        doc_lid_output_path,
        verbose: bool = True,
    ):
        print("Starting SETU LID Segregation Spark Pipeline...........")

        df = df.select(cols_to_use) \
                .filter(df.successful_extraction == True)

        df = self.set_split_count_and_salt(df, docs_per_partition)
        df = self.doc_clean_stage(df, cols_to_use, text_col, doc_id_col, verbose)
        df = self.lid_stage(df, doc_id_col, text_col)

        # Duplicate the doc_lang column as doc_lang_partition
        df = df.withColumn("doc_lang_partition", col("doc_lang"))

        df.write.partitionBy("doc_lang_partition").mode("overwrite") \
            .parquet(doc_lid_output_path)

        rename_partitioned_directories(doc_lid_output_path, "doc_lang_partition")

        print(f"Completed `doc_lang` level `df` parquet write.... to: {doc_lid_output_path}")


    def run_analysis_spark_pipeline(
        self,
        df,
        doc_id_col,
        text_col,
        docs_per_partition,
        line_stats_output_path,
        doc_stats_output_path,
        analysis_output_path,
        verbose:bool = True,
    ):

        print("Starting SETU Analysis Spark Pipeline...........")

        df = self.set_split_count_and_salt(df, docs_per_partition)
        line_df = self.convert_to_line(df, text_col, verbose)
        line_df = self.line_stats_collection(line_df, text_col, line_stats_output_path, verbose)
        doc_stats_df = self.aggregate_to_doc_stats(line_df, doc_id_col, text_col, True,  verbose)
        df = self.convert_to_doc(df, line_df, text_col, doc_id_col, verbose)
        doc_stats_df = self.collect_repetition_scores(df, doc_stats_df, doc_id_col, text_col, verbose)

        doc_stats_df.drop(text_col) \
                    .join(df.select(doc_id_col, "doc_lang"), [doc_id_col]) \
                    .withColumn("doc_lang_partition", col("doc_lang")) \
                    .write.partitionBy("doc_lang_partition") \
                    .mode("overwrite") \
                    .parquet(doc_stats_output_path)

        rename_partitioned_directories(doc_stats_output_path, "doc_lang_partition")

        if verbose:
            doc_stats_df.show(n=5)

        print(f"Completed doc-level `doc_stats_df` parquet write.... to: {doc_stats_output_path}")

        df.withColumn("doc_lang_partition", col("doc_lang")) \
            .write.partitionBy("doc_lang_partition").mode("overwrite") \
            .parquet(analysis_output_path)

        rename_partitioned_directories(analysis_output_path, "doc_lang_partition")

        print(f"Completed analysis `df` parquet write.... to: {analysis_output_path}")

    def run_plotting(
        doc_stats_df,
        save_plot_directory,
        verbose: bool = True,
    ):
        doc_stats_df = doc_stats_df.toPandas()
        


    def run_flagging_and_filtering_spark_pipeline(
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

        doc_stats_df = doc_stats_df.select("*", self.has_char_repetition_udf("char_ngram_repetition_score").alias("has_char_repetition")) \
                                    .select("*", self.has_word_repetition_udf("word_ngram_repetition_score").alias("has_word_repetition")) \
                                    .drop("char_ngram_repetition_score", "word_ngram_repetition_score")

        if verbose:
            doc_stats_df.show(n=5)
            print("Completed `has_char_reptition` & `has_word_reptition`....")

        if self.config.line_count_filter:

            doc_stats_df = doc_stats_df.filter(doc_stats_df.has_less_lines == False)

            if verbose:
                doc_stats_df.show(n=5)
                print("Completed `is_number` removal filter....")

        if self.config.line_length_filter:

            doc_stats_df = doc_stats_df.filter(doc_stats_df.is_short_lines_heavy == False)

            if verbose:
                doc_stats_df.show(n=5)
                print("Completed `is_number` removal filter....")

        if self.config.nsfw_filter:

            if self.config.save_nsfw_data:
                doc_stats_df.filter(doc_stats_df.is_nsfw_heavy == True) \
                            .write.mode("overwrite") \
                            .parquet(nsfw_output_path if nsfw_output_path else self.config.nsfw_output_path)

                if verbose:
                    nsfw_df.show(n=5)
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

    def remove_documents(
        self,
        df,
        doc_stats_df,
        docs_per_partition,
        doc_id_col,
        filtered_docs_path,
    ):
        print("Starting SETU Document Removal Spark Pipeline...........")
        
        df = self.set_split_count_and_salt(df, docs_per_partition)
        doc_stats_df = self.salting(doc_stats_df, self.n_splits)
        df = df.join(doc_stats_df, [doc_id_col, "doc_lang"])

        df.write.mode("overwrite") \
                .parquet(filtered_docs_path)

        print(f"Completed `filtered_docs` parquet write...written to: {filtered_docs_path}")


    def run_pipeline(
        self,
        doc,
        use_code_filter=True,
        use_terminal_punc_filter=True,
        enable_analysis=True,
        enable_flagging=True,
        lid_probability_threshold: float = 0.7,
        chunk_len_threshold: int = 2,
        non_li_threshold: float = 1.0,
        nsfw_threshold: float = 1.0,
        symbol_number_threshold: float = 1.0,
        min_line_count: int = 0,
        min_mean_line_len: int = 0,
        word_ngram_cum_thresholds: Dict[str, float] = {
            "6": 1.0,
            "7": 1.0,
            "8": 1.0,
            "9": 1.0
        },
        char_ngram_cum_thresholds: Dict[str, float] = {
            "5": 1.0,
            "6": 1.0,
            "7": 1.0,
            "8": 1.0
        },
        **kwargs,
    ):
        
        doc = dict(doc)

        outputs = {}

        doc["code_spans"] = find_code_spans(doc["text"])
        doc["has_code"] = has_code(doc["code_spans"])

        code_span_cleaned_text = None
        terminal_cleaned_text = None

        if use_code_filter:
            doc["text"] = remove_code(doc["text"], doc["has_code"], doc["code_spans"])
            code_span_cleaned_text = doc["text"]

        if use_terminal_punc_filter:
            doc["text"], doc["chunks_flagged"] = terminal_punc_filter(doc["text"], chunk_len_threshold)
            terminal_cleaned_text = doc["text"]

        if not len(doc["text"]):
            return outputs, code_span_cleaned_text, terminal_cleaned_text

        doc["lid_major"], doc["lid_all"] = self.lid.run_lid_single(
            doc["text"].replace("\n", " "), 
            for_spark=False, 
            lid_probability_threshold=lid_probability_threshold
        )

        doc["iso"] = self.lid.get_iso_code(doc["lid_major"][0])
        lines = split_with_delimiter(doc["text"])

        if enable_analysis:
            metadata_jsons = []
            for line in lines:
                metadata_jsons += [
                    extract_line_metadata(
                        doc["id"],
                        doc["source"],
                        line,
                        doc["lid_major"][0],
                        doc["iso"],
                        doc["url"],
                    )
                ]
            outputs["analysis"] = extract_document_metadata(
                doc_id=doc["id"],
                source=doc["source"],
                line_stats_list=metadata_jsons,
                lang=doc["lid_major"][0],
                lang_code=doc["iso"],
                text_key="text",
                nsfw_count_key="nsfw_words_count",
                words_count_key="words_count",
                char_count_key="char_count",
                non_li_key="non_li_count",
                bytes_key="bytes",
                symbol_number_count_key="symbol_number_count",
                word_ngrams=tuple(map(int, list(word_ngram_cum_thresholds.keys()))),
                char_ngrams=tuple(map(int, list(char_ngram_cum_thresholds.keys()))),
                url=doc["url"],
            )
            outputs["analysis"]["lid_major"] = doc["lid_major"]
            outputs["analysis"]["lid_all"] = doc["lid_all"]    
            outputs["analysis"]["iso"] = doc["iso"]        
            outputs["analysis"]["code_spans"] = doc["code_spans"]

        if enable_flagging:
            
            outputs["flags"] = perform_doc_flagging(
                outputs["analysis"],
                min_line_count = min_line_count,
                min_mean_line_len = min_mean_line_len,
                nsfw_threshold = nsfw_threshold,
                symbol_number_threshold = symbol_number_threshold,    
                non_li_threshold = non_li_threshold,
                word_ngram_cum_thresholds = word_ngram_cum_thresholds,
                char_ngram_cum_thresholds = char_ngram_cum_thresholds
            )
            outputs["flags"]["has_code"] = doc["has_code"]

        return outputs, code_span_cleaned_text, terminal_cleaned_text, lines


if __name__ == "__main__":

    setu = Setu(config_file="/mnt/phallm-data/priyam/setu/pipeline/configs/dashboard_config.json")

    import pandas as pd

    df = pd.read_parquet('/mnt/phallm-data/datasets/sangraha/parquets-trafilatura/malayalam/0.parquet', engine='pyarrow')

    sample = df.iloc[0]

    output = setu.run_pipeline(
        sample,
        use_terminal_punc_filter=True,
        enable_analysis=True,
        enable_flagging=True,
        lid_probability_threshold=0.7,
        chunk_len_threshold=2,
        non_li_threshold=1.0,
        nsfw_threshold=1.0,
        symbol_number_threshold=1.0,
        min_line_count=0,
        min_mean_line_len=0,
        word_ngram_cum_thresholds={
            "6": 1.0,
            "7": 1.0,
            "8": 1.0,
            "9": 1.0
        },
        char_ngram_cum_thresholds={
            "5": 1.0,
            "6": 1.0,
            "7": 1.0,
            "8": 1.0
        },
    )

    print(output["flagging"])     