import argparse
import subprocess
from core import (
    SetuStage,
    str2bool, 
    list_of_strings,
    ChunkHandler,
    SparkOptimizedHandlers
)
from pyspark.sql import Window
from pyspark.sql.functions import count, col, row_number
from pyspark.sql.types import (
    LongType,
    IntegerType,
    StringType, 
    StructType,
    StructField
)

class Page2PDFStage(SetuStage):
    
    def __init__(self, config):
        super().__init__(config)

        self.chunk_handler = ChunkHandler()
        self.spark_optimized_handler = SparkOptimizedHandlers()

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--p2p_parquets_path",
            type=str,
            help="Path to the parquets containing page text",
        )

        parser.add_argument(
            "--p2p_doc_stats_path",
            type=str,
            help="Path to the parquets containing page stats aka output of analysis stage",
        )

        parser.add_argument(
            "--p2p_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--p2p_data_quality",
            type=str,
            choices=["cleaned", "uncleaned"],
            help="Data quality of text",
        )

        parser.add_argument(
            "--p2p_text_col",
            type=str,
            default="text",
            help="Name of the column containing the text",
        )

        parser.add_argument(
            "--p2p_lang_col",
            type=str,
            required=False,
            help="Name of the column containing the language",
        )

        parser.add_argument(
            "--p2p_identifier_col",
            type=str,
            default="url",
            help="Name of the column containing the a unique identifier per page.",
        )

        parser.add_argument(
            "--p2p_groupby_col",
            type=str,
            default="url",
            help="Name of the column containing the identifier used for grouping all the pages of a pdf",
        )

        parser.add_argument(
            "--p2p_sort_col",
            type=str,
            default="page_no",
            help="Name of the column containing the order or page number for sorting the pages in right order",
        )

        parser.add_argument(
            "--p2p_symbol_join",
            type=str,
            default="",
            # default="<<PaGe_EnD>>",
            help="Symbol to use while joining the pages of a pdf",
        )

        parser.add_argument(
            "--p2p_doc_stats_output_path",
            type=str,
            help="Path to the folder to store pdf output",
        )

        parser.add_argument(
            "--p2p_doc_output_path",
            type=str,
            help="Path to the folder to store pdf output",
        )

        return parser

    def aggregate_to_pdf_stats(
        self, 
        page_df, 
        doc_id_col, 
        bytes_col="bytes",
        words_col="words_count",
        char_col="char_count",
        drop_repeated_line_dist=True, 
        only_base_stats=False, 
        verbose=False,
    ):

        doc_stats_df = self.spark_optimized_handler.run_analysis(
            line_df=page_df,
            doc_id_col=doc_id_col,
            line_nsfw_count_col_="nsfw_words_count",
            line_non_li_count_col_="non_li_char_count",
            line_bytes_col_=bytes_col,
            line_words_count_col_=words_col,
            line_char_count_col_=char_col,
            only_base_stats=only_base_stats,
        )

        if drop_repeated_line_dist:
            doc_stats_df = doc_stats_df.drop("repeated_line_dist")

        doc_stats_df.cache()

        if verbose:
            doc_stats_df.show(n=5)
            print("Completed `page2pdf` run with metadata/stats aggregation....")

        return doc_stats_df

    def detect_pdf_lang(self, df, groupby_col, lang_col):

        # Group by both url and lang and then count the occurrences for each combination
        df_grouped = df.groupBy(groupby_col, lang_col).agg(count("*").alias("count"))

        # Define a window specification which partitions by the 'url' and orders by 'count' in descending order
        windowSpec = Window.partitionBy(groupby_col).orderBy(col("count").desc())

        # Add a row_number over the window specification
        df_ranked = df_grouped.withColumn("rank", row_number().over(windowSpec))

        # Filter out the most frequently occurring language for each url
        df_result = df_ranked.filter(col("rank") == 1).drop("count", "rank")

        return df_result
    

    def run_stage_parallelized(
        self,
        df,
        doc_stats_df,
        docs_per_partition,
        data_quality,
        text_col,
        groupby_col,
        sort_col,
        lang_col,
        identifier_col,
        join_symbol,
        p2p_doc_output_path,
        p2p_doc_stats_output_path,
    ):
        print("Starting SETU Page2PDF Spark Pipeline...........")

        df = self.set_split_count_and_salt(df, docs_per_partition)        

        df = df.withColumn(sort_col, col(sort_col).cast('int'))

        pdf_df = self.chunk_handler.lines2doc(df, text_col, groupby_col, sort_col, join_symbol)

        pdf_df = self.salting(pdf_df, self.n_splits)

        # pdf_df.show(5)

        if data_quality == "uncleaned":

            pdf_df.write.mode("overwrite") \
                        .parquet(p2p_doc_output_path)

            print(f"Completed `page2pdf` parquet write...written to: {p2p_doc_output_path}")

            doc_stats_df = self.salting(doc_stats_df, self.n_splits)

            pdf_stats_df = self.aggregate_to_pdf_stats(
                page_df=doc_stats_df,
                doc_id_col=groupby_col, 
                bytes_col="uncleaned_bytes",
                words_col="uncleaned_words_count",
                char_col="uncleaned_chars_count",
                drop_repeated_line_dist=True,
                only_base_stats=True,
                verbose=False,
            )
            pdf_stats_df.write.mode("overwrite") \
                        .parquet(p2p_doc_stats_output_path)

        elif data_quality == "cleaned":

            lang_df = self.detect_pdf_lang(df, groupby_col, lang_col)   

            lang_df = self.salting(lang_df, self.n_splits)

            pdf_df = pdf_df.join(lang_df, [groupby_col])         

            pdf_df.write.mode("overwrite") \
                        .parquet(p2p_doc_output_path)

            print(f"Completed `page2pdf` parquet write...written to: {p2p_doc_output_path}")
        
            doc_stats_df = self.salting(doc_stats_df, self.n_splits)

            doc_stats_df = doc_stats_df.join(
                df.select(identifier_col, groupby_col),
                [identifier_col]
            )

            pdf_stats_df = self.aggregate_to_pdf_stats(
                page_df=doc_stats_df,
                doc_id_col=groupby_col, 
                drop_repeated_line_dist=True,
                verbose=False,
            )

            pdf_stats_df = pdf_stats_df.join(lang_df, [groupby_col])

            pdf_stats_df.write.mode("overwrite") \
                        .parquet(p2p_doc_stats_output_path)            


    def run_data_parallelized(
        self, 
        **kwargs
    ):
        raise NotImplementedError(f"`run_data_parallelized` function has not been implemented for class `{self.name}`")

    def run_spark(
        self,
        spark,
        p2p_parquets_path,
        p2p_samples_per_partition,
        p2p_data_quality,
        p2p_doc_stats_path,
        p2p_text_col,
        p2p_groupby_col,
        p2p_sort_col,
        p2p_lang_col,
        p2p_identifier_col,
        p2p_symbol_join,
        p2p_doc_stats_output_path,
        p2p_doc_output_path,
        run_local,
    ):
        df = spark.read.format("parquet").load(p2p_parquets_path)
        doc_stats_df = spark.read.format("parquet").load(p2p_doc_stats_path)

        return self.run_stage_parallelized(
            df=df,
            doc_stats_df=doc_stats_df,
            docs_per_partition=p2p_samples_per_partition,
            data_quality=p2p_data_quality,
            text_col=p2p_text_col,
            groupby_col=p2p_groupby_col,
            sort_col=p2p_sort_col,
            lang_col=p2p_lang_col,
            identifier_col=p2p_identifier_col,
            join_symbol=p2p_symbol_join,
            p2p_doc_output_path=p2p_doc_output_path,
            p2p_doc_stats_output_path=p2p_doc_stats_output_path,
        )
        
    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs) 

    def run_normal(
        self, 
        p2p_parquets_path,
        p2p_samples_per_partition,
        p2p_data_quality,
        p2p_doc_stats_path,
        p2p_text_col,
        p2p_groupby_col,
        p2p_sort_col,
        p2p_lang_col,
        p2p_identifier_col,
        p2p_symbol_join,
        p2p_doc_stats_output_path,
        p2p_doc_output_path,
        run_local,
    ):
        raise NotImplementedError(f"`run_normal` function has not been implemented for class `{self.name}`")