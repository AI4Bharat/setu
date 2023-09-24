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
# from constants import (
    # constants,
    # init_kwprs
# )
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
from lid import (
    LIDPipeline,
    run_lid_on_each_partition_with_idx,
    run_lid_spark_pipeline,
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
# import matplotlib.pyplot as plt

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
            # self.constants = Constants(self.config.filter_data_root)
            # self.kw_processors = init_kwprs(self.constants)
    
    def load_config(self, config_file):

        tmp_location = config_file
        
        if config_file.startswith("gs://"):
            tmp_location = "/tmp/" + os.path.split(config_file)[1]
            subprocess.run(["gsutil", "cp", config_file, tmp_location])

        with open(tmp_location, "r") as config_f:
            config_= json.load(config_f)
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

        # df.show(1000)

        # Repartition based on the salted key to ensure more even distribution
        df = df.repartition(n_splits, "salt")

        df = df.drop("salt")

        return df

    def set_split_count_and_salt(self, df, docs_per_partition):

        self.df_total_rows = df.count()

        self.n_splits = ceil(self.df_total_rows/docs_per_partition)

        print(f"When required data will be repartitioned into - {self.n_splits} partitions")

        df = self.salting(df, self.n_splits)

        return df

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

    def lid_stage(self, spark, df, id_cols, text_col):
        df = self.salting(df, self.n_splits)
        df.cache()
        df = run_lid_spark_pipeline(spark, self.config, df, id_cols, text_col, "doc_lang", "doc_lang_iso")
        df.cache()
        df.localCheckpoint()
        df.show(n=5)
        print("Completed `doc_lang`....")
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

    def run_doc_clean_spark_pipeline(
        self,
        spark,
        df,
        cols_to_use,
        doc_id_col,
        text_col,   
        docs_per_partition,
        use_symbol_filter,
        save_symbol_heavy_docs,
        symbol_filter_output_path,
        cleaned_doc_output_path,
        run_data_parallel_mode,
        verbose: bool = True,
    ):

        df = df.filter(df.successful_extraction == True) \
                .dropDuplicates([doc_id_col]) \
                .select(cols_to_use)


        print(f"Count after filtering for extraction: {df.count()}")

        df = df.na.drop(subset=[text_col])

        print(f"Count after filtering for extraction: {df.count()}")

        df = self.set_split_count_and_salt(df, docs_per_partition)

        df.withColumn("partitionId",spark_partition_id()).groupBy("partitionId").count().sort("partitionId").show(df.rdd.getNumPartitions())

        if run_data_parallel_mode:

            print("Running `doc_clean` stage in data-parallel mode....")

            df = self.run_data_parallelized_doc_clean(
                spark, df, cols_to_use, doc_id_col, text_col,   
                docs_per_partition, use_symbol_filter, 
                save_symbol_heavy_docs, symbol_filter_output_path,
                cleaned_doc_output_path, verbose,
            )
        else:
            df = self.doc_clean_stage(
                df, cols_to_use, text_col, doc_id_col,
                use_symbol_filter, save_symbol_heavy_docs,
                symbol_filter_output_path, verbose
            )

        df = self.salting(df, self.n_splits)

        df.write.mode("overwrite").parquet(cleaned_doc_output_path)

        print(f"Completed `doc_clean` level `df` parquet write.... to: {cleaned_doc_output_path}")

    def run_data_parallelized_doc_clean(
        self,
        spark,
        df,
        cols_to_use,
        doc_id_col,
        text_col,   
        docs_per_partition,
        use_symbol_filter,
        save_symbol_heavy_docs,
        symbol_filter_output_path,
        cleaned_doc_output_path,
        verbose: bool = True,
    ):
        
        def clean_docs(
            idx, 
            partition, 
            doc_id_col,
            text_col,
            use_symbol_filter,
            symbol_filter_output_path
        ):

            print(f"Performing Document Cleaning on partition {idx}......")

            os.makedirs(symbol_filter_output_path, exist_ok=True) 
            symbol_heavy_parquet_path = os.path.join(symbol_filter_output_path, f"{idx}.parquet")
            symbol_heavy_schema = pa.schema([
                (doc_id_col, pa.string()),
                ("url", pa.string()),
                ("source", pa.string()),
                (text_col, pa.string()),
                ("language", pa.string()),
                ("code_spans", pa.list_(pa.list_(pa.int64()))),
                ("uncleaned_chars_count", pa.int64()),
                ("uncleaned_words_count", pa.int64()),
                ("uncleaned_bytes", pa.int64()),
                ("symbol_ratio", pa.float64()),
                ("invalid_char_count", pa.int64()),
            ])

            symbol_heavy_out = {
                doc_id_col: [],
                "url": [],
                "source": [],
                text_col: [],
                "language": [],
                "code_spans": [],
                "uncleaned_chars_count": [],
                "uncleaned_words_count": [],
                "uncleaned_bytes": [],
                "symbol_ratio": [],
                "invalid_char_count": [],
            }
            
            for row in partition:
                # print(f"Doc ID: {row[doc_id_col]}")
                # print(f"Text: {row[text_col]}")
                code_spans = find_code_spans(row[doc_id_col], row[text_col])
                if self.config.remove_code:
                    text = remove_code(row[text_col], code_spans)

                # print(f"After remove code: {text}")
                
                if not len(text):
                    continue

                uncleaned_chars_count = get_char_count(text)
                uncleaned_words_count = get_word_count(text)
                uncleaned_bytes = get_bytes(text)

                symbol_ratio, invalid_char_count = get_symbol_ratio(text, uncleaned_chars_count)

                if use_symbol_filter and symbol_ratio >=self.config.symbol_threshold :
                    symbol_heavy_out[doc_id_col] += [row[doc_id_col]]
                    symbol_heavy_out["url"] += [row["url"]]
                    symbol_heavy_out["source"] += [row["source"]]
                    symbol_heavy_out[text_col] += [row[text_col]]
                    symbol_heavy_out["language"] += [row["language"]]
                    symbol_heavy_out["code_spans"] += [code_spans]
                    symbol_heavy_out["uncleaned_chars_count"] += [uncleaned_chars_count]
                    symbol_heavy_out["uncleaned_words_count"] += [uncleaned_words_count]
                    symbol_heavy_out["uncleaned_bytes"] += [uncleaned_bytes]
                    symbol_heavy_out["symbol_ratio"] += [symbol_ratio]
                    symbol_heavy_out["invalid_char_count"] += [invalid_char_count]
                else:
                    chunks = text.split("\n")
                    terminal_valid = tuple(map(is_terminal_valid, chunks))
                    cleaned_text = ""
                    for i, terminal_valid_check in enumerate(terminal_valid):
                        if terminal_valid_check:
                            cleaned_text += chunks[i] + "\n"
                    
                    if not len(cleaned_text):
                        continue

                    res_list = [
                        row[doc_id_col], row["url"], row["source"], row["language"], code_spans,
                        uncleaned_chars_count, uncleaned_words_count, uncleaned_bytes, symbol_ratio, 
                        invalid_char_count, text, cleaned_text
                    ]

                    yield res_list

            if use_symbol_filter:

                symbol_heavy_table = pa.table(symbol_heavy_out, schema=symbol_heavy_schema)

                with pq.ParquetWriter(
                    symbol_heavy_parquet_path, symbol_heavy_table.schema, compression="SNAPPY"
                ) as pq_writer:
                    pq_writer.write_table(symbol_heavy_table)

            print(f"Written symbol-heavy parquet file of partition {idx} -> {symbol_heavy_parquet_path}......")

        result_schema = StructType([
            StructField(doc_id_col, StringType(), True),
            StructField("url", StringType(), True), 
            StructField("source", StringType(), True),
            StructField("language", StringType(), True),
            StructField("code_spans", ArrayType(ArrayType(IntegerType())), True), 
            StructField("uncleaned_chars_count", IntegerType(), True),
            StructField("uncleaned_words_count", IntegerType(), True),
            StructField("uncleaned_bytes", IntegerType(), True),
            StructField("symbol_ratio", FloatType(), True),
            StructField("invalid_char_count", IntegerType(), True),
            StructField("uncleaned_text", StringType(), True),
            StructField(text_col, StringType(), True),
        ])

        clean_docs_dp = partial(
            clean_docs, 
            doc_id_col=doc_id_col,
            text_col=text_col,
            use_symbol_filter=use_symbol_filter,
            symbol_filter_output_path=symbol_filter_output_path,
        )

        cleaned_doc_rdd = df.rdd.mapPartitionsWithIndex(clean_docs_dp)
        cleaned_doc_df = spark.createDataFrame(cleaned_doc_rdd, schema=result_schema)
        return cleaned_doc_df

    def run_lid_segregation_spark_pipeline(
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

        df = df.select(doc_id_col, text_col, *additional_cols_to_use)
        df = self.set_split_count_and_salt(df, docs_per_partition)
        df = self.lid_stage(spark, df, [doc_id_col] + additional_cols_to_use, text_col)
        df = self.salting(df, self.n_splits)

        # Duplicate the doc_lang column as doc_lang_partition
        df = df.withColumn("doc_lang_partition", col("doc_lang"))

        df.write.partitionBy("doc_lang_partition").mode("overwrite") \
            .parquet(doc_lid_output_path)

        # rename_partitioned_directories(doc_lid_output_path, "doc_lang_partition")

        print(f"Completed `doc_lang` level `df` parquet write.... to: {doc_lid_output_path}")

    def run_analysis_spark_pipeline(
        self,
        df,
        cols_to_use,
        doc_id_col,
        text_col,
        docs_per_partition,
        line_stats_output_path,
        doc_stats_output_path,
        analysis_output_path,
        verbose:bool = True,
    ):

        print("Starting SETU Analysis Spark Pipeline...........")

        df = df.select(cols_to_use)

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

        # rename_partitioned_directories(doc_stats_output_path, "doc_lang_partition")

        if verbose:
            doc_stats_df.show(n=5)

        print(f"Completed doc-level `doc_stats_df` parquet write.... to: {doc_stats_output_path}")

        df.withColumn("doc_lang_partition", col("doc_lang")) \
            .write.partitionBy("doc_lang_partition").mode("overwrite") \
            .parquet(analysis_output_path)

        # rename_partitioned_directories(analysis_output_path, "doc_lang_partition")

        print(f"Completed analysis `df` parquet write.... to: {analysis_output_path}")

    def run_data_parallelized_analysis(
        self,
        data_iter,
        cols_to_use,
        doc_id_col,
        text_col,
        docs_per_partition,
        line_stats_output_path,
        doc_stats_output_path,
        analysis_output_path,
        verbose:bool = True,
    ):
        # TODO: If REQURIED, code this up. 
        # Currently, analysis module is working fine after 
        # implementing data-parallelism mode for cleaning stage.
        # Hypothesising that data skews generated during
        # Trafilatura Extraction are handled at cleaning stage.

        pass


    def run_plotting(
        doc_stats_df,
        save_plot_directory,
        verbose: bool = True,
    ):
        # TODO: If POSSIBLE, code this up. 
        # Currently left as proper plots are needing manual inspection

        pass

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
                print("Completed `has_less_lines` removal filter....")

        if self.config.line_length_filter:

            doc_stats_df = doc_stats_df.filter(doc_stats_df.is_short_lines_heavy == False)

            if verbose:
                doc_stats_df.show(n=5)
                print("Completed `is_short_lines_heavy` removal filter....")

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

        df = df.join(doc_stats_df.drop("doc_lang"), [doc_id_col], "inner")

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