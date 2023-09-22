from base import SetuComponent
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
    length,
    spark_partition_id,
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
    find_code_spans_spark,
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
    get_symbol_ratio,
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

from utilities import (
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
import subprocess

find_code_spans_udf = udf(
    find_code_spans_spark, 
    StructType([
        StructField("code_spans", ArrayType(ArrayType(IntegerType())), True),
        StructField("code_spans_success", BooleanType(), True)
    ]),
)
get_symbol_ratio_udf = udf(
    partial(get_symbol_ratio, for_spark=True),
    StructType([
        StructField("symbol_ratio", FloatType(), True),
        StructField("invalid_char_count", IntegerType(), True),
    ])
)
is_terminal_valid_udf = udf(is_terminal_valid, BooleanType()) # Doc level
split_with_delimiter_udf = udf(split_with_delimiter, ArrayType(StringType()))
get_nsfw_word_dist_udf = udf(get_nsfw_word_dist, MapType(StringType(), IntegerType()))
non_li_chars_total_count_udf = udf(non_li_chars_total_count, IntegerType())
get_word_count_udf = udf(get_word_count, IntegerType())
get_char_count_udf = udf(get_char_count, IntegerType())
get_bytes_udf = udf(get_bytes, IntegerType())

class CrawlAnalysisComponent(SetuComponent):

    def __init__(self, config):
        super().__init__(config)
        self.wrap_funcs_to_udf()
    
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

    
    def doc_clean_stage(self, df, cols_to_use, text_col, doc_id_col,
                        use_symbol_filter, save_symbol_heavy_docs, 
                        symbol_filter_output_path, verbose):

        curr_cols = list(df.schema.names)
        
        df = df.select("*", find_code_spans_udf(doc_id_col, text_col).alias("code_span_results")) \
                .select(*curr_cols, "code_span_results.*")
        
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

        df = df.select("*", length(text_col).alias("uncleaned_chars_count"))
        df = df.select("*", get_word_count_udf(text_col).alias("uncleaned_words_count"))
        df = df.select("*", get_bytes_udf(text_col).alias("uncleaned_bytes"))

        curr_cols = list(df.schema.names)

        df = df.select("*", get_symbol_ratio_udf(text_col, "uncleaned_chars_count").alias("symbol_ratio_results")) \
                .select(*curr_cols, "symbol_ratio_results.*")

        if use_symbol_filter:

            if save_symbol_heavy_docs:

                    df.filter(df.symbol_ratio >=self.config.symbol_threshold) \
                        .write.mode("overwrite") \
                        .parquet(symbol_filter_output_path)

                    # rename_partitioned_directories(symbol_filter_output_path, "doc_lang_partition")

                    print(f"Completed `symbol heavy df` parquet write.... to: {symbol_filter_output_path}")

            df = df.filter(df.symbol_ratio < self.config.symbol_threshold)

        df.show(n=5)

        df = self.salting(df, self.n_splits)
                
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
            .withColumn("uncleaned_text", col(text_col)) \
            .drop(text_col) \
            .join(doc_df, [doc_id_col])

        doc_df.unpersist(True)

        if verbose:
            df.show(n=5, truncate=False)

        return df
