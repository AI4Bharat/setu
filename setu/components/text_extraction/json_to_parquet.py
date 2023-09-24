import argparse
from core import SetuStage, str2bool
import subprocess
from pyspark.sql.types import (
    StringType, 
    StructType, 
    StructField
)
from pyspark.sql.functions import lit, rand
import glob
import json
from functools import partial

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
            "--j2p_parquet_output_path",
            type=str,
            help="directory where parquets will be stored",
        )

        return parser

    def run_stage_parallelized(
        self,
        json_list,
        docs_per_partition,
        output_path,
        lang,
    ):
        
        # Currently, written assuming the json structure
        # will be same across all sources: `crawl`, `ocr` & `asr`.
        
        json_schema = StructType([
            StructField("doc_id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("source", StringType(), True),
            StructField("text", StringType(), True),
        ])
        json_df = spark.read.json(json_list, schema=json_schema)
        json_df = json_df.select("doc_id", "url", "source", lit(lang).alias("language"), "text")
        json_df = self.set_split_count_and_salt(json_df, docs_per_partition)
        json_df = json_df.dropDuplicates([doc_id_col])
        json_df.write.mode("overwrite") \
            .parquet(output_path)

    def convert_crawl_output(
        self,
        spark,
        json_list,
        docs_per_partition,
        output_path,
        lang,
        run_local
    ):

        def read_files_from_list(idx, partition, lang, run_local=True):

            print(f"Starting processing of partition - {idx}")

            if not run_local:
                tmp_list = [f"{i}\n" for i in json_list]
                with open(f"/tmp/json_data_{self.mode}_{idx}.txt", "w") as tmp_f:
                    tmp_f.writelines(tmp_list)
                subprocess.run([
                    f"cat /tmp/json_data_{self.mode}_{idx}.txt",
                    "|",
                    "gsutil", 
                    "-m",
                    "cp", 
                    "-I", 
                    f"/tmp/json_data_{self.mode}_{idx}"
                ])
                json_list = glob.glob(f"/tmp/json_data_{self.mode}/*.json")

            for row in partition:
                json_path = row["value"]
                print(f"Performing extraction on: {json_path}")
                with open(json_path, "r") as jf:
                    content = json.load(jf)
                out = [content["doc_id"], content["url"], content["source"], lang, content["text"]]
                yield out

        save_parquets = partial(
            read_files_from_list, lang=lang, run_local=run_local
        )

        jsons_path_df = spark.createDataFrame(json_list, StringType())
        # curr_cols = list(jsons_path_df.schema.names)
        json_path_df = self.set_split_count_and_salt(jsons_path_df, docs_per_partition)
        parquet_rdd = json_path_df.rdd.mapPartitionsWithIndex(save_parquets)

        result_schema = StructType([
            StructField("doc_id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("source", StringType(), True),
            StructField("language", StringType(), True),
            StructField("text", StringType(), True),
        ])
        df = spark.createDataFrame(parquet_rdd, schema=result_schema)
        df = self.salting(df, self.n_splits)
        df.write.mode("overwrite") \
            .parquet(output_path)

    def convert_ocr_output(
        self,
        spark,
        json_list,
        docs_per_partition,
        output_path,
        lang,
        run_local
    ):
        # Currently, code for `convert_crawl_output` and `convert_ocr_output` is same.
        # But, keeping 2 separate function just in case I need to revisit it in future,
        # separate the 2.

        return self.convert_crawl_output(
            spark=spark,
            json_list=json_list,
            docs_per_partition=docs_per_partition,
            output_path=output_path,
            lang=lang,
            run_local=run_local,
        )

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
        language,
        j2p_samples_per_partition,
        j2p_verbose,
        j2p_run_mode,
        j2p_parquet_output_path,
        run_local,
    ):
        json_list = glob.glob(json_glob_path)
        if j2p_run_mode == "stage":
            return self.run_stage_parallelized(
                json_list=json_list,
                docs_per_partition=j2p_samples_per_partition,
                output_path=j2p_parquet_output_path,
                lang=language,
            )
        elif j2p_run_mode == "data":
            return self.run_data_parallelized(
                spark=spark,
                json_list=json_list,
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
        language,
        j2p_samples_per_partition,
        j2p_verbose,
        j2p_run_mode,
        j2p_parquet_output_path,
        run_local,
    ):
        raise NotImplementedError("`run_normal` function has not been implemented for class `JSON2Parquet`")
 