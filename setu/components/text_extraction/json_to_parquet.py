import argparse
from core import SetuStage, str2bool, list_of_strings
import subprocess
from pyspark.sql.types import (
    StringType, 
    StructType, 
    StructField
)
from pyspark.sql.functions import lit, rand, col
import glob
import json
from functools import partial
from google.cloud import storage
import os

class JSON2ParquetStage(SetuStage):

    def __init__(self, config, mode=None):
        super().__init__(config)

        if mode not in ["crawl", "ocr", "asr"]:
            raise ValueError(f"`mode` is given value: {mode}. Only 3 values allowed:{['crawl', 'ocr', 'asr']}")
        else:
            self.mode = mode

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--json_glob_path",
            type=str,
            required=True,
            help="Glob expression to resolve for getting JSON file list that needs to be converted to parquet"
        )

        parser.add_argument(
            "--j2p_cols",
            type=list_of_strings,
            help="`,` separated Columns to use as identifiers",
        )

        parser.add_argument(
            "--language",
            type=str,
            required=True,
            help="JSONs contain text data of which language?"
        )

        parser.add_argument(
            "--j2p_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--j2p_verbose",
            type=str2bool,
            default=False,
            required=False,
            help="Whether to add `show()`",
        )

        parser.add_argument(
            "--j2p_run_mode",
            type=str,
            required=False,
            choices=["data", "stage"],
            default="data",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--j2p_is_multiline",
            type=str2bool,
            default=False,
            required=False,
            help="Whether to jsons are multiline or not",
        )

        parser.add_argument(
            "--j2p_parquet_output_path",
            type=str,
            help="directory where parquets will be stored",
        )

        parser.add_argument(
            "--j2p_bucket",
            type=str,
            default=None,
            required=False,
            help="gcp bucket containing json",
        )

        parser.add_argument(
            "--j2p_bucket_prefix",
            type=str,
            default=None,
            required=False,
            help="gcp bucket prefix pointing to jsons",
        )

        return parser

    def run_stage_parallelized(
        self,
        spark,
        json_glob,
        cols,
        docs_per_partition,
        doc_id_col,
        is_multiline,
        output_path,
        lang,
    ):
        
        # Currently, written assuming the json structure
        # will be same across all sources: `crawl`, `ocr` & `asr`.
        
        json_df = spark.read.format("json").options(multiline=is_multiline, ignoreCorruptFiles=True).schema(self.schema_creator(cols)).load(json_glob)
        if "body" in cols:
            json_df = json_df.select("*", col("body").alias("text")).drop("body")
        if self.mode == "crawl":
            json_df = json_df.select("doc_id", "url", "source", lit(lang).alias("language"), "text")
            if "URL" in cols:
                json_df = json_df.select("*", col("URL").alias("url")).drop("URL")
        elif self.mode == "ocr":
            json_df = json_df.select("*", lit(lang).alias("language"))
        
        self.df_total_rows = json_df.count()
        print(f"Initial Count: {self.df_total_rows}")
        json_df = self.set_split_count_and_salt(json_df, docs_per_partition)
        json_df = json_df.dropDuplicates([doc_id_col])
        print(f"Count after removing duplicates: {json_df.count()}")
        json_df.write.mode("overwrite") \
            .parquet(output_path)

    def schema_creator(self, cols):
        schema = []
        for col in cols:
            schema += [StructField(col, StringType(), True)]
        schema = StructType(schema)
        return schema

    def convert_crawl_output(
        self,
        spark,
        json_list,
        j2p_bucket,
        docs_per_partition,
        output_path,
        lang,
        run_local
    ):

        # def read_files_from_list(idx, partition, lang, run_local=True):

        #     print(f"Starting processing of partition - {idx}")

        #     if not run_local:
        #         tmp_list = [f"{i}\n" for i in json_list]
        #         with open(f"/tmp/json_data_{self.mode}_{idx}.txt", "w") as tmp_f:
        #             tmp_f.writelines(tmp_list)
        #         subprocess.run([
        #             f"cat",
        #             f"/tmp/json_data_{self.mode}_{idx}.txt",
        #             "|",
        #             "gsutil", 
        #             "-m",
        #             "cp",
        #             "-I", 
        #             f"/tmp/json_data_{self.mode}_{idx}"
        #         ])
        #         json_list = glob.glob(f"/tmp/json_data_{self.mode}_{idx}/*.json")

        #     for row in partition:
        #         json_path = row["value"]
        #         print(f"Performing extraction on: {json_path}")
        #         with open(json_path, "r") as jf:
        #             content = json.load(jf)
        #         out = [content["doc_id"], content["url"], content["source"], lang, content["text"]]
        #         yield out

        # save_parquets = partial(
        #     read_files_from_list, lang=lang, run_local=run_local
        # )

        # jsons_path_df = spark.createDataFrame(json_list, StringType())
        # # curr_cols = list(jsons_path_df.schema.names)
        # json_path_df = self.set_split_count_and_salt(jsons_path_df, docs_per_partition)
        # parquet_rdd = json_path_df.rdd.mapPartitionsWithIndex(save_parquets)

        # result_schema = StructType([
        #     StructField("doc_id", StringType(), True),
        #     StructField("url", StringType(), True),
        #     StructField("source", StringType(), True),
        #     StructField("language", StringType(), True),
        #     StructField("text", StringType(), True),
        # ])
        # df = spark.createDataFrame(parquet_rdd, schema=result_schema)
        # df = self.salting(df, self.n_splits)
        # df.write.mode("overwrite") \
        #     .parquet(output_path)

        pass

    def convert_ocr_output(
        self,
        spark,
        json_list,
        j2p_bucket,
        docs_per_partition,
        output_path,
        lang,
        run_local
    ):

        def read_files_from_list(idx, partition, lang, run_local=True):

            print(f"Starting processing of partition - {idx}")

            if run_local:
                for row in partition:
                    json_path = row["value"]
                    print(f"Performing extraction on: {json_path}")
                    with open(json_path, "r") as jf:
                        content = json.load(jf)
                    out = [
                        content["doc_id"],
                        content["url"],
                        content["source"],
                        lang, 
                        str(content["page_no"]),
                        content["identifier"],
                        content["pdf_name"],
                        content["text"]
                    ]
                    yield out
            else:
                client = storage.Client()
                bucket = client.get_bucket(j2p_bucket)
                tmp_dir = f"/tmp/json_data_{self.mode}_{idx}"
                os.makedirs(tmp_dir, exist_ok=True)
                for i, row in enumerate(partition):
                    json_path = row["value"]
                    tmp_path = os.path.join(tmp_dir, f"{i}.json")
                    blob = bucket.blob(json_path)
                    blob.download_to_filename(tmp_path)
                    print(f"Performing extraction on: {json_path} which is downloaded at {tmp_path}")
                    with open(tmp_path, "r") as jf:
                        content = json.load(jf)
                    out = [
                        content["doc_id"], 
                        content["url"], 
                        content["source"], 
                        lang,
                        str(content["page_no"]), 
                        content["identifier"], 
                        content["pdf_name"],
                        content["text"]
                    ]
                    yield out

        save_parquets = partial(
            read_files_from_list, lang=lang, run_local=run_local
        )

        jsons_path_df = spark.createDataFrame(json_list, StringType())
        json_path_df = self.set_split_count_and_salt(jsons_path_df, docs_per_partition)
        parquet_rdd = json_path_df.rdd.mapPartitionsWithIndex(save_parquets)

        result_schema = self.schema_creator(["doc_id","url","source","language","page_no","identifier","pdf_name","text"])
        df = spark.createDataFrame(parquet_rdd, schema=result_schema)
        df.show(5)
        df = self.salting(df, self.n_splits)
        df.write.mode("overwrite") \
            .parquet(output_path)

    def convert_asr_output(self, **kwargs):
        raise NotImplementedError("`convert_asr_output` function has not been implemented for class `JSON2Parquet`")

    def run_data_parallelized(self, **kwargs):
        if self.mode == "crawl":
            return self.convert_crawl_output(**kwargs)
        elif self.mode == "ocr":
            return self.convert_ocr_output(**kwargs)
        elif self.mode == "asr":
            return self.convert_asr_output(**kwargs)

    def run_spark(
        self,
        spark,
        json_glob_path,
        j2p_cols,
        language,
        j2p_samples_per_partition,
        j2p_verbose,
        j2p_run_mode,
        j2p_is_multiline,
        j2p_parquet_output_path,
        run_local,
        j2p_bucket,
        j2p_bucket_prefix
    ):
        
        if j2p_run_mode == "stage":
            return self.run_stage_parallelized(
                spark=spark,
                json_glob=json_glob_path,
                cols=j2p_cols,
                docs_per_partition=j2p_samples_per_partition,
                doc_id_col="doc_id",
                is_multiline=j2p_is_multiline,
                output_path=j2p_parquet_output_path,
                lang=language,
            )

        json_list = []
        if run_local and j2p_run_mode == "data":
            json_list = glob.glob(json_glob_path)
        elif not run_local and j2p_run_mode == "data":
            storage_client = storage.Client()
            # Get the bucket
            bucket = storage_client.bucket(j2p_bucket)
            # List all the blobs in the bucket
            blobs = bucket.list_blobs(prefix=j2p_bucket_prefix)
            for blob in blobs:
                # print(blob.name)
                json_list += [blob.name]

        print("TOTAL PAGE COUNT to Process: ", len(json_list))

        if j2p_run_mode == "data":
            return self.run_data_parallelized(
                spark=spark,
                json_list=json_list,
                j2p_bucket=j2p_bucket,
                is_multiline=j2p_is_multiline,
                docs_per_partition=j2p_samples_per_partition,
                output_path=j2p_parquet_output_path,
                lang=language,
                run_local=run_local,
            )
        else:
            raise Exception("Incorrect input for `j2p_run_mode`. `j2p_run_mode` only supports 2 types: `stage` & `data`.")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs)

    def run_normal(
        self, 
        json_glob_path,
        j2p_cols,
        language,
        j2p_samples_per_partition,
        j2p_verbose,
        j2p_run_mode,
        j2p_parquet_output_path,
        run_local,
        j2p_bucket,
        j2p_bucket_prefix
    ):
        raise NotImplementedError("`run_normal` function has not been implemented for class `JSON2Parquet`")
 