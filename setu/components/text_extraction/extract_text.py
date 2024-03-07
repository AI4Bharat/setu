import argparse
from core import SetuStage, str2bool
import subprocess
from pyspark.sql.functions import col
from pyspark.sql.types import (
    BooleanType,
    StringType, 
    StructType, 
    StructField,
    Row
)
import glob
import trafilatura
from bs4 import BeautifulSoup

class ExtractTextStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--te_parquets_path",
            type=str,
            help="Path of input parquets for text extraction"
        )

        parser.add_argument(
            "--te_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--te_verbose",
            type=str2bool,
            default=False,
            required=False,
            help="Whether to add `show()`",
        )

        parser.add_argument(
            "--te_run_mode",
            type=str,
            required=False,
            choices=["data"],
            default="data",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--te_output_path",
            type=str,
            help="Directory where all the extracted text parquets will be stored."
        )

        return parser

    def run_stage_parallelized(
        self,
        df,
        docs_per_partition,
        output_path,
    ):
        
        # TODO: Look at trafilatura output json and set correct Types in udf schema.
        # Once done uncomment the entire section below

        raise NotImplementedError("`run_stage_parallelized` function has not been implemented for class `TextExtractionStage`")
        
        
        # def trafilatura_extraction_spark(html, url):
        #     out = []
        #     res = trafilatura.bare_extraction(html, include_images=False)
        #     if res:
        #         print("Extraction complete. Now, appending values.")
        #         out["successful_extraction"].append(True) 
        #         for col in traf_cols:
        #             out += [res[col]]
        #     else:
        #         print(f"Trafilatura Output: `None`. Not able to extraction text from: {url}.")
        #         out["successful_extraction"].append(False)
        #         for col in traf_cols:
        #             out += [None]
        #     return Row(
        #         "successful_extraction","title","description",
        #         "text","comments","author","hostname","sitename",
        #         "date","categories","tags","fingerprint","id",
        #         "license","body","commentsbody","raw_text","image",
        #         "pagetype")(*out)

        # te_udf = udf(
        #     trafilatura_extraction, 
        #     StructType([
        #         StructField("successful_extraction", BooleanType(), False),
        #         StructField("title", StringType(), True),
        #         StructField("description", StringType(), True),
        #         StructField("text", StringType(), True),
        #         StructField("comments", StringType(), True),
        #         StructField("author", StringType(), True),
        #         StructField("hostname", StringType(), True),
        #         StructField("sitename", StringType(), True),
        #         StructField("date", StringType(), True),
        #         StructField("categories", StringType(), True),
        #         StructField("tags", StringType(), True),
        #         StructField("fingerprint", StringType(), True),
        #         StructField("id", StringType(), True),
        #         StructField("license", StringType(), True),
        #         StructField("body", StringType(), True),
        #         StructField("commentsbody", StringType(), True),
        #         StructField("raw_text", StringType(), True),
        #         StructField("image", StringType(), True),
        #         StructField("pagetype", StringType(), True),
        #     ])
        # )

        # df = df.dropDuplicates(["doc_id"])
        # df = self.set_split_count_and_salt(df, docs_per_partition)
        # df = df.withColumn("html", col("text")).drop("text")
        # curr_cols = ["doc_id", "url", "source", "language"]
        # df = df.select("*", te_udf("html", "url").alias("te_results")) \
        #         .select(*curr_cols, "te_results.*")
        # df = self.salting(df, self.n_split)
        # df.write.mode("overwrite") \
        #     .parquet(output_path)
           

    def run_data_parallelized(
        self,
        spark,
        df,
        docs_per_partition,
        output_path,
    ):
        def extract_text_trafilatura(idx, partition):

            traf_cols = [
                "title","description","text","comments","author",
                "hostname","sitename","date","categories","tags",
                "fingerprint","id","license","body","commentsbody",
                "raw_text","image","pagetype"
            ]

            print(f"Starting processing of partition - {idx}")

            for row in partition:
                out = []
                for col in ["doc_id", "url", "source", "timestamp", "language"]:
                    out += [row[col]]
                print(f"Performing extraction on: {row['url']}")
                try:
                    if bool(BeautifulSoup(row["text"], "html.parser").find()):
                        res = trafilatura.bare_extraction(row['text'], include_images=False)
                    else:
                        res = None
                except Exception as e:
                    print(f"Faced issues witb extracting for URL: {row['url']}. Encountered error: {e}")
                    res = None
                if res:
                    print("Extraction complete. Now, appending values.")
                    out += [True] 
                    for col in traf_cols:
                        out += [res[col]]
                else:
                    print(f"Trafilatura Output: `None`. Not able to extraction text from: {row['url']}.")
                    out += [False]
                    for col in traf_cols:
                        out += [None]

                yield out

        df = df.dropDuplicates(["doc_id"])
        df = df.na.drop(subset=["timestamp"])
        df = self.set_split_count_and_salt(df, docs_per_partition)
        te_rdd = df.rdd.mapPartitionsWithIndex(extract_text_trafilatura)

        result_schema = StructType([
            StructField("doc_id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("source", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("language", StringType(), True),
            StructField("successful_extraction", BooleanType(), False),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("text", StringType(), True),
            StructField("comments", StringType(), True),
            StructField("author", StringType(), True),
            StructField("hostname", StringType(), True),
            StructField("sitename", StringType(), True),
            StructField("date", StringType(), True),
            StructField("categories", StringType(), True),
            StructField("tags", StringType(), True),
            StructField("fingerprint", StringType(), True),
            StructField("id", StringType(), True),
            StructField("license", StringType(), True),
            StructField("body", StringType(), True),
            StructField("commentsbody", StringType(), True),
            StructField("raw_text", StringType(), True),
            StructField("image", StringType(), True),
            StructField("pagetype", StringType(), True),
        ])
        df = spark.createDataFrame(te_rdd, schema=result_schema)
        df = self.salting(df, self.n_splits)
        df.write.mode("overwrite") \
            .parquet(output_path)

    def run_spark(
        self,
        spark,
        te_parquets_path,
        te_samples_per_partition,
        te_verbose,
        te_run_mode,
        te_output_path,
        run_local,
    ):

        df = spark.read.format("parquet").load(te_parquets_path)

        if te_run_mode == "stage":
            return self.run_stage_parallelized(
                df=df,
                docs_per_partition=te_samples_per_partition,
                output_path=te_output_path,
            )
        elif te_run_mode == "data":
            return self.run_data_parallelized(
                spark=spark,
                df=df,
                docs_per_partition=te_samples_per_partition,
                output_path=te_output_path,
            )
        else:
            raise Exception("Incorrect input for `te_run_mode`. `te_run_mode` only supports 2 types: `stage` & `data`.")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs) 

    def run_normal(
        self,
        te_parquets_path,
        te_samples_per_partition,
        te_verbose,
        te_run_mode,
        te_output_path,
        run_local,
    ):
        raise NotImplementedError("`run_normal` function has not been implemented for class `TextExtractionStage`")