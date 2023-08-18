from typing import Dict
import numpy as np
from pyspark.sql import SparkSession
from pyspark import TaskContext
from pyspark.sql.functions import (
    udf,
    posexplode,
    size,
    when
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
from utils import ChunkHandler, SparkOptimizedHandlers
from argparse import Namespace
import pyarrow as pa
import pyarrow.parquet as pq
import argparse
import os
from math import ceil
import time

find_code_spans_udf = udf(
    find_code_spans, 
    StructType([StructField("code_spans",
                            ArrayType(ArrayType(IntegerType())), True),
                StructField("code_spans_success",
                            BooleanType(), True)]))
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

    def run_spark_pipeline(
        self,
        df,
        cols_to_use,
        doc_id_col,
        text_col,
        docs_per_partition,
        save_doc_lid_output,
        doc_lid_output_path,
        save_line_stats_output,
        line_stats_output_path,
        save_doc_stats_output,
        doc_stats_output_path,
        save_nsfw_data,
        nsfw_output_path,
        final_output_path,
        run_analysis = True,
        run_flagging = True,
        doc_stats_df = None,
        verbose:bool = True, 
    ):

        print("Starting Spark Pipeline...........")

        if run_analysis:

            df = df \
                .select(cols_to_use) \
                .filter(df.successful_extraction == True)

            self.df_total_rows = df.count()

            self.n_splits = ceil(self.df_total_rows/docs_per_partition)

            print(f"When required data will be repartitioned into - {self.n_splits} partitions")

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

            spans_df = self.chunk_handler.doc2lines(df, text_col, "\n")

            if verbose:
                spans_df.explain(mode="formatted")
                spans_df.show(n=5, truncate=False)
                print("Completed `doc2lines` via `\\n`....")

            spans_df = spans_df.select("*", is_terminal_valid_udf(text_col).alias("is_terminal_valid"))

            if verbose:
                spans_df.explain(mode="formatted")
                spans_df.show(n=5, truncate=False)
                print("Completed `is_terminal_valid`....")
            
            if self.config.remove_terminal_invalid:

                spans_df = spans_df.filter(spans_df.is_terminal_valid == True)

                if verbose:
                    spans_df.explain(mode="formatted")
                    spans_df.show(n=5, truncate=False)
                    print("Completed `remove_non_terminal_punc_span`....")

            doc_df = self.chunk_handler.lines2doc(spans_df, text_col, doc_id_col, "pos", '\n')

            if verbose:
                doc_df.show(n=5, truncate=False)
                print("Completed `lines2doc` via ` `....")

            df = df \
                .drop(text_col) \
                .join(doc_df, [doc_id_col])

            if verbose:
                df.show(n=5, truncate=False)
                print("Updated Text column with cleaned text....")

            prev_num_of_partitions = df.rdd.getNumPartitions()

            df = df.repartition(self.n_splits)

            df = run_lid_spark_pipeline(self.config, df, [doc_id_col], text_col, "doc_lang", "doc_lang_iso")

            df = df.repartition(prev_num_of_partitions)

            if verbose:
                df.show(n=5)
                print("Completed `doc_lang`....")

            if self.config.save_doc_lid_output:

                df \
                .write \
                .mode("overwrite") \
                .parquet(doc_lid_output_path if doc_lid_output_path else self.config.doc_lid_output_path)

                print("Completed `doc_lang` level `df` parquet write....")

            line_df = df.withColumn(text_col, split_with_delimiter_udf(text_col))

            if verbose:
                line_df.show(n=5)
                print("Completed `split_at_terminal` ....")

            line_df = line_df.select("*", posexplode(text_col)).drop(text_col).withColumnRenamed("col", text_col)

            if verbose:
                line_df.show(n=5)
                print("Completed `posexplode` to get line-level `df` ....")


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
        
            line_df = line_df.select("*", self.is_numbers_udf(text_col, "doc_lang").alias("is_number"))

            if verbose:
                line_df.show(n=5)
                print("Completed `is_number`....")
            

            line_df = line_df.select("*", non_li_chars_total_count_udf(text_col).alias("non_li_char_count"))

            if verbose:
                line_df.show(n=5)
                print("Completed `non_li_char_count`....")
            
            if self.config.save_line_stats_output:
                line_df \
                .write \
                .mode("overwrite") \
                .parquet(line_stats_output_path if line_stats_output_path else self.config.line_stats_output_path)

                print("Completed line-level `df` parquet write....")
            
            if self.config.remove_only_number:
                line_df = line_df.filter(line_df.is_number == True)
                
                if verbose:
                    line_df.show(n=5)
                    print("Completed `is_number` removal filter....")

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

            if verbose:
                doc_stats_df.show(n=5)
                print("Completed `line2doc` run with metadata/stats aggregation....")

            doc_df = self.chunk_handler.lines2doc(line_df, text_col, doc_id_col, "pos", " ")

            if verbose:
                doc_df.show(n=5)
                print("Completed `lines2doc` via ` `....")
            
            df = df \
                .drop(text_col) \
                .join(doc_df, [doc_id_col])    

            if verbose:    
                doc_stats_df.show(n=5)
                print("Updated Text column with cleaned text....")

            if verbose:
                df.show(n=5)
                print("Completed `join` for doc and doc_stats via `doc_id`....")

            df = df.select("*", self.get_char_ngram_repetition_udf(text_col).alias("char_ngram_repetition_score"))

            if verbose:
                df.show(n=5)
                print("Completed `char_ngram_reptition_score`....")

            df = df.select("*", self.get_word_ngram_repetition_udf(text_col, "doc_lang_iso").alias("word_ngram_repetition_score"))

            if verbose:
                df.show(n=5)
                print("Completed `word_ngram_reptition_score`....")

            df = df.drop("local_path", "repeated_line_dist")

        if run_flagging:

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

            if verbose:
                doc_stats_df.show(n=5)
                print("Completed `doc_flagging`....")

            df = df.join(doc_stats_df, [doc_id_col])

            df = df.select("*", self.has_char_repetition_udf("char_ngram_repetition_score").alias("has_char_repetition"))

            if verbose:
                df.show(n=5)
                print("Completed `has_char_reptition`....")

            df = df.select("*", self.has_word_repetition_udf("word_ngram_repetition_score").alias("has_word_repetition"))

            if verbose:
                df.show(n=5)
                print("Completed `has_word_reptition`....")

            if self.config.save_doc_stats_output:
                df \
                .drop(text_col) \
                .write \
                .mode("overwrite") \
                .parquet(doc_stats_output_path if doc_stats_output_path else self.config.doc_stats_output_path)

                if verbose:
                    df.show(n=5)
                    print("Completed doc-level `df` parquet write....")

            if self.config.line_count_filter:

                df = df.filter(df.has_less_lines == False)

                if verbose:
                    df.show(n=5)
                    print("Completed `is_number` removal filter....")

            if self.config.line_length_filter:

                df = df.filter(df.is_short_lines_heavy == False)

                if verbose:
                    df.show(n=5)
                    print("Completed `is_number` removal filter....")

            if self.config.nsfw_filter:

                if self.config.save_nsfw_data:
                    df.filter(df.is_nsfw_heavy == True) \
                        .write \
                        .mode("overwrite") \
                        .parquet(nsfw_output_path if nsfw_output_path else self.config.nsfw_output_path)

                    if verbose:
                        nsfw_df.show(n=5)
                        print("Completed nsfw `df` parquet write....")

                df = df.filter(df.is_nsfw_heavy == False)
                
                if verbose:
                    df.show(n=5)                

            if self.config.non_li_filter:

                df = df.filter(df.is_non_li_heavy == False)

                if verbose:
                    df.show(n=5)
                    print("Completed `is_non_li_heavy` removal filter....")

            if self.config.word_repetition_filter:

                df = df.filter(df.has_word_repetition == False)

                if verbose:
                    df.show(n=5)
                    print("Completed `has_word_repetition` removal filter....")
                

            if self.config.char_repetition_filter:
                
                df = df.filter(df.has_char_repetition == False)

                if verbose:
                    df.show(n=5)
                    print("Completed `has_char_repetition` removal filter....")

            if verbose:
                df.show(n=5)
                print("Completed doc-level `df` parquet write....")

        df \
        .write \
        .mode("overwrite") \
        .parquet(final_output_path if final_output_path else self.config.final_output_path)

        print("Completed final `df` parquet write....")

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