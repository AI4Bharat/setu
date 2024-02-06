import argparse
import subprocess
from core import (
    SetuStage,
    ChunkHandler, 
    SparkOptimizedHandlers, 
    rename_partitioned_directories,
    CONSTANTS, 
    KW_PROCESSORS,
    str2bool
)
from pyspark.sql.functions import (
    udf, 
    col, 
    length, 
    spark_partition_id,
    count
)
from pyspark.sql.types import (
    BooleanType,
    IntegerType, 
    ArrayType, 
    StringType, 
    FloatType,
    StructType,
    StructField
)
from functools import partial
from .document_filters import (
    find_code_spans,
    find_code_spans_spark,
    get_symbol_ratio,
    is_num_or_punc_only,
    has_code,
    remove_code,
    is_terminal_valid,
    remove_non_terminal_punc_span,
    terminal_punc_filter,
    split_at_terminal_punc,
)

from .line_filters import (
    get_word_count,
    get_char_count,
    get_bytes
)

import pyarrow as pa
import pyarrow.parquet as pq
import os

find_code_spans_udf = udf(
    find_code_spans_spark, 
    StructType([
        StructField("code_spans", ArrayType(ArrayType(IntegerType())), True),
        StructField("code_spans_success", BooleanType(), True)
    ]),
)
remove_code_udf = udf(remove_code, StringType())
get_symbol_ratio_udf = udf(
    partial(get_symbol_ratio, for_spark=True),
    StructType([
        StructField("symbol_ratio", FloatType(), True),
        StructField("invalid_char_count", IntegerType(), True),
    ])
)
is_num_or_punc_only_udf = udf(is_num_or_punc_only, BooleanType())
is_terminal_valid_udf = udf(is_terminal_valid, BooleanType())
get_word_count_udf = udf(get_word_count, IntegerType())
get_char_count_udf = udf(get_char_count, IntegerType())
get_bytes_udf = udf(get_bytes, IntegerType())

class DocCleanStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

        if self.spark_present:
            self.chunk_handler = ChunkHandler()

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--doc_df_parquets_path",
            type=str,
            help="Path to folder containing parquets",
        )

        parser.add_argument(
            "--is_doc_df_path_batched",
            type=str2bool,
            required=False,
            default=True,
            help="Is path a batch path or not?",
        )

        parser.add_argument(
            "--doc_clean_additional_cols_to_use",
            type=lambda x: x.split(","),
            required=True,
            help="`,` separated column names"
        )

        parser.add_argument(
            "--use_symbol_filter",
            type=str2bool,
            required=False,
            default=True,
            help="Whether to use symbol filter",
        )

        parser.add_argument(
            "--doc_clean_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--doc_clean_verbose",
            type=str2bool,
            default=False,
            required=False,
            help="Whether to add `show()`",
        )

        parser.add_argument(
            "--doc_clean_run_mode",
            type=str,
            required=False,
            choices=["data", "stage"],
            default="data",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--save_symbol_heavy_docs",
            type=str2bool,
            required=False,
            default=True,
            help="Whether to save documents filtered by symbol filter",
        )

        parser.add_argument(
            "--symbol_filter_output_path",
            type=str,
            help="Path of the folder store symbol filter flagged documents",
        )

        parser.add_argument(
            "--cleaned_doc_output_path",
            type=str,
            help="Path of the folder store cleaned doc checkpoint",
        )

        return parser

    def doc_clean_stage(self, df, text_col, doc_id_col,
                        use_symbol_filter, save_symbol_heavy_docs, 
                        symbol_filter_output_path, verbose):

        curr_cols = list(df.schema.names)
        
        if verbose:
            df.explain(mode="formatted")
            df.show(n=5)
            print("Completed `find_code_spans`....")

        if self.config.remove_code:

            df = df.select("*", find_code_spans_udf(doc_id_col, text_col).alias("code_span_results")) \
                    .select(*curr_cols, "code_span_results.*")

            df = df.withColumn(text_col, remove_code_udf(text_col, "code_spans"))

            if verbose:
                df.explain(mode="formatted")
                df.show(n=5)
                print("Completed `remove_code`....")

        df = df.select("*", length(text_col).alias("uncleaned_chars_count"))
        df = df.select("*", get_word_count_udf(text_col).alias("uncleaned_words_count"))
        df = df.select("*", get_bytes_udf(text_col).alias("uncleaned_bytes"))

        curr_cols = list(df.schema.names)

        if use_symbol_filter:

            df = df.select("*", get_symbol_ratio_udf(text_col, "uncleaned_chars_count").alias("symbol_ratio_results")) \
                    .select(*curr_cols, "symbol_ratio_results.*")

            if save_symbol_heavy_docs:

                    df.filter(df.symbol_ratio >=self.config.symbol_threshold) \
                        .write.mode("overwrite") \
                        .parquet(symbol_filter_output_path)

                    print(f"Completed `symbol heavy df` parquet write.... to: {symbol_filter_output_path}")

            df = df.filter(df.symbol_ratio < self.config.symbol_threshold)

        df.show(n=5)

        df = self.salting(df, self.n_splits)
                
        spans_df = self.chunk_handler.doc2lines(df, text_col, "\n")

        if verbose:
            spans_df.explain(mode="formatted")
            spans_df.show(n=5)
            print("Completed `doc2lines` via `\\n`....")

        if self.config.remove_only_num_or_punc_chunks:
            spans_df = spans_df.withColumn("is_num_or_punc_only", is_num_or_punc_only_udf(text_col)) \
                                .filter(col("is_num_or_punc_only") == False)

        if self.config.repeated_chunk_filter:
            filtered_chunks = spans_df.groupBy("url", text_col) \
                                    .agg(count("*").alias("repetition_count")) \
                                    .filter(col("repetition_count") == 1)
                                    
            spans_df = spans_df.join(
                filtered_chunks.select("url", "text"), 
                ["url", "text"]
            )

        df = self.salting(df, self.n_splits)
        
        if self.config.remove_terminal_invalid:
            spans_df = spans_df.select("*", is_terminal_valid_udf(text_col).alias("is_terminal_valid")) \
                                .filter(spans_df.is_terminal_valid == True)

        if self.config.chunk_length_filter:
            spans_df = spans_df.withColumn("chunk_word_count", get_word_count_udf(text_col)) \
                                .filter(col("chunk_word_count") > 1)

        if verbose:
            spans_df.explain(mode="formatted")
            spans_df.show(n=5)
            print("Completed all `chunk cleaning filters`")

        doc_df = self.chunk_handler.lines2doc(spans_df, text_col, doc_id_col, "pos", '\n')

        doc_df = self.salting(doc_df, self.n_splits)

        if verbose:
            doc_df.show(n=5)
            print("Completed `lines2doc` via ` `....")

        df = df \
            .withColumn("uncleaned_text", col(text_col)) \
            .drop(text_col) \
            .join(doc_df, on=[doc_id_col], how="left")

        doc_df.unpersist(True)

        if verbose:
            df.show(n=5, truncate=False)

        return df

    def run_preprocessing(
        self,
        df,
        additional_cols_to_use,
        doc_id_col,
        text_col,
        docs_per_partition        
    ):

        if "successful_extraction" in additional_cols_to_use and "successful_extraction" in list(df.schema.names):
            df = df.filter(df.successful_extraction == True)

        df = df.dropDuplicates([doc_id_col]) \
                .select(doc_id_col, text_col, *additional_cols_to_use)

        print(f"Count after filtering for extraction: {df.count()}")

        df = df.na.drop(subset=[text_col])

        self.df_total_rows = df.count()

        print(f"Count after filtering for `None` values {text_col} col: {self.df_total_rows}")

        df = self.set_split_count_and_salt(df, docs_per_partition)

        df.withColumn("partitionId",spark_partition_id()).groupBy("partitionId").count().sort("partitionId").show(df.rdd.getNumPartitions())

        return df

    def run_stage_parallelized(
        self,
        df,
        additional_cols_to_use,
        doc_id_col,
        text_col,   
        docs_per_partition,
        use_symbol_filter,
        save_symbol_heavy_docs,
        symbol_filter_output_path,
        cleaned_doc_output_path,
        verbose: bool = True,
    ):

        df = self.run_preprocessing(
            df=df,
            additional_cols_to_use=additional_cols_to_use,
            doc_id_col=doc_id_col,
            text_col=text_col,
            docs_per_partition=docs_per_partition
        )

        df = self.doc_clean_stage(
            df=df, 
            doc_id_col=doc_id_col,
            text_col=text_col, 
            use_symbol_filter=use_symbol_filter, 
            save_symbol_heavy_docs=save_symbol_heavy_docs,
            symbol_filter_output_path=symbol_filter_output_path,
            verbose=verbose
        )

        df = self.salting(df, self.n_splits)

        df.write.mode("overwrite").parquet(cleaned_doc_output_path)

        print(f"Completed `doc_clean` level `df` parquet write.... to: {cleaned_doc_output_path}")

    def run_data_parallelized(
        self,
        spark,
        df,
        additional_cols_to_use,
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
            additional_cols_to_use,
            use_symbol_filter,
            symbol_filter_output_path,
        ):

            print(f"Performing Document Cleaning on partition {idx}......")

            os.makedirs(symbol_filter_output_path, exist_ok=True) 
            symbol_heavy_parquet_path = os.path.join(symbol_filter_output_path, f"{idx}.parquet")
            symbol_heavy_schema = pa.schema(
                [ (col, pa.string()) for col in [doc_id_col, text_col] + additional_cols_to_use ]
                +
                [
                    ("code_spans", pa.list_(pa.list_(pa.int32()))),
                    ("uncleaned_chars_count", pa.int32()),
                    ("uncleaned_words_count", pa.int32()),
                    ("uncleaned_bytes", pa.int32()),
                    ("symbol_ratio", pa.float32()),
                    ("invalid_char_count", pa.int32()),
                ]
            )

            symbol_heavy_out = {
                "code_spans": [],
                "uncleaned_chars_count": [],
                "uncleaned_words_count": [],
                "uncleaned_bytes": [],
                "symbol_ratio": [],
                "invalid_char_count": [],
            }
            for col in [doc_id_col, text_col] + additional_cols_to_use:
                symbol_heavy_out[col] = []
            
            for row in partition:
                code_spans = find_code_spans(row[doc_id_col], row[text_col])
                if self.config.remove_code:
                    text = remove_code(row[text_col], code_spans)
                
                if not len(text):
                    continue

                uncleaned_chars_count = get_char_count(text)
                uncleaned_words_count = get_word_count(text)
                uncleaned_bytes = get_bytes(text)

                symbol_ratio, invalid_char_count = get_symbol_ratio(text, uncleaned_chars_count)

                if use_symbol_filter and symbol_ratio >=self.config.symbol_threshold :
                    for col in [doc_id_col, text_col] + additional_cols_to_use:
                        symbol_heavy_out[col] += [str(row[col])]
                    symbol_heavy_out["code_spans"] += [code_spans]
                    symbol_heavy_out["uncleaned_chars_count"] += [uncleaned_chars_count]
                    symbol_heavy_out["uncleaned_words_count"] += [uncleaned_words_count]
                    symbol_heavy_out["uncleaned_bytes"] += [uncleaned_bytes]
                    symbol_heavy_out["symbol_ratio"] += [symbol_ratio]
                    symbol_heavy_out["invalid_char_count"] += [invalid_char_count]
                else:
                    chunks = text.split("\n")
                    cleaned_text = ""
                    for i, chunk in enumerate(chunks):
                        
                        is_num_or_punc_valid = True
                        if self.config.remove_only_num_or_punc_chunks:
                            if is_num_or_punc_only(chunk):
                                is_num_or_punc_valid = False

                        is_chunk_terminal_valid = True
                        if self.config.remove_terminal_invalid:
                            if not is_terminal_valid(chunk):
                                is_chunk_terminal_valid = False

                        is_chunk_long_enough = True
                        if self.config.chunk_length_filter:
                            if get_word_count(chunk) <= 1:
                                is_chunk_long_enough = False
                    
                        if is_num_or_punc_valid and is_chunk_terminal_valid and is_chunk_long_enough:
                            cleaned_text += chunk + "\n"

                    if not len(cleaned_text):
                        cleaned_text = None

                    res_list = [row[doc_id_col], cleaned_text]
                    for col in additional_cols_to_use:
                        res_list += [str(row[col])]
                    res_list += [
                        code_spans, uncleaned_chars_count,
                        uncleaned_words_count, uncleaned_bytes,
                        symbol_ratio, invalid_char_count, text
                    ]

                    yield res_list

            if use_symbol_filter:

                symbol_heavy_table = pa.table(symbol_heavy_out, schema=symbol_heavy_schema)

                with pq.ParquetWriter(
                    symbol_heavy_parquet_path, symbol_heavy_table.schema, compression="SNAPPY"
                ) as pq_writer:
                    pq_writer.write_table(symbol_heavy_table)

            print(f"Written symbol-heavy parquet file of partition {idx} -> {symbol_heavy_parquet_path}......")

        df = self.run_preprocessing(
            df=df,
            additional_cols_to_use=additional_cols_to_use,
            doc_id_col=doc_id_col,
            text_col=text_col,
            docs_per_partition=docs_per_partition
        )

        result_schema = StructType(
            [
                StructField(col, StringType(), True) for col in [doc_id_col, text_col] + additional_cols_to_use
            ]
            +
            [
                StructField("code_spans", ArrayType(ArrayType(IntegerType())), True),
                StructField("uncleaned_chars_count", IntegerType(), True),
                StructField("uncleaned_words_count", IntegerType(), True),
                StructField("uncleaned_bytes", IntegerType(), True),
                StructField("symbol_ratio", FloatType(), True),
                StructField("invalid_char_count", IntegerType(), True),
                StructField("uncleaned_text", StringType(), True),
            ]
        )

        clean_docs_dp = partial(
            clean_docs, 
            doc_id_col=doc_id_col,
            text_col=text_col,
            additional_cols_to_use=additional_cols_to_use,
            use_symbol_filter=use_symbol_filter,
            symbol_filter_output_path=symbol_filter_output_path,
        )

        cleaned_doc_rdd = df.rdd.mapPartitionsWithIndex(clean_docs_dp)
        cleaned_doc_df = spark.createDataFrame(cleaned_doc_rdd, schema=result_schema)

        cleaned_doc_df = self.salting(cleaned_doc_df, self.n_splits)

        if self.config.repeated_chunk_filter:

            spans_df = self.chunk_handler.doc2lines(cleaned_doc_df, text_col, "\n")

            filtered_chunks = spans_df.groupBy("url", text_col) \
                                    .agg(count("*").alias("repetition_count")) \
                                    .filter(col("repetition_count") == 1)
                                    
            spans_df = spans_df.join(
                filtered_chunks.select("url", "text"), 
                ["url", "text"]
            )

            doc_df = self.chunk_handler.lines2doc(spans_df, text_col, doc_id_col, "pos", '\n')

            cleaned_doc_df = cleaned_doc_df \
                                .drop(text_col) \
                                .join(doc_df, on=[doc_id_col], how="left")

        cleaned_doc_df.write.mode("overwrite").parquet(cleaned_doc_output_path)

        print(f"Completed `doc_clean` level `cleaned_doc_df` parquet write.... to: {cleaned_doc_output_path}")

    def run_spark(
        self,
        spark,
        doc_df_parquets_path,
        is_doc_df_path_batched,
        doc_clean_additional_cols_to_use,
        use_symbol_filter,
        doc_clean_samples_per_partition,
        doc_clean_verbose,
        doc_clean_run_mode,
        save_symbol_heavy_docs,
        symbol_filter_output_path,
        cleaned_doc_output_path,
        run_local,
    ):

        if is_doc_df_path_batched:
            if not run_local:
                subprocess.run([[
                    "gsutil",
                    "cp",
                    doc_df_parquets_path,
                    "/tmp/doc_clean_batch.info"
                ]])
            with open("/tmp/doc_clean_batch.info", "r") as batch_f:
                parquet_list = [line.strip() for line in batch_f.readlines()]
            doc_df = spark.read.format("parquet").load(parquet_list)
        else:
            doc_df = spark.read.format("parquet").load(doc_df_parquets_path)

        if doc_clean_run_mode == "stage":
            return self.run_stage_parallelized(
                df=doc_df,
                additional_cols_to_use=doc_clean_additional_cols_to_use,
                doc_id_col="doc_id",
                text_col="text",
                docs_per_partition=doc_clean_samples_per_partition,
                use_symbol_filter=use_symbol_filter,
                save_symbol_heavy_docs=save_symbol_heavy_docs,
                symbol_filter_output_path=symbol_filter_output_path,
                cleaned_doc_output_path=cleaned_doc_output_path,
                verbose=doc_clean_verbose,
            )
        elif doc_clean_run_mode == "data":
            return self.run_data_parallelized(
                spark=spark,
                df=doc_df,
                additional_cols_to_use=doc_clean_additional_cols_to_use,
                doc_id_col="doc_id",
                text_col="text",
                docs_per_partition=doc_clean_samples_per_partition,
                use_symbol_filter=use_symbol_filter,
                save_symbol_heavy_docs=save_symbol_heavy_docs,
                symbol_filter_output_path=symbol_filter_output_path,
                cleaned_doc_output_path=cleaned_doc_output_path,
                verbose=doc_clean_verbose,
            )
        else:
            raise Exception("Incorrect input for `doc_clean_run_mode`. `doc_clean_run_mode` only supports 2 types: `stage` & `data`.")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs)

    def run_normal(
        self,
        doc_df_parquets_path,
        is_doc_df_path_batched,
        doc_clean_additional_cols_to_use,
        use_symbol_filter,
        doc_clean_samples_per_partition,
        doc_clean_verbose,
        doc_clean_run_mode,
        save_symbol_heavy_docs,
        symbol_filter_output_path,
        cleaned_doc_output_path,
        run_local,
    ):
        raise NotImplementedError("`run_normal` function has not been implemented for class `DocCleanStage`")