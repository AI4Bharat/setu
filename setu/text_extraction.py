from argparse import Namespace,ArgumentParser
from base import SetuComponent,SetuStage
from utilities import str2bool,list_of_strings
import subprocess
from pyspark.sql.types import (
    StringType, 
    StructType, 
    StructField,
    BooleanType
)
from pyspark.sql.functions import lit, rand, col
import glob
import json
from functools import partial
from google.cloud import storage
import os
from pyspark.sql import SparkSession,DataFrame
from typing import Callable
from bs4 import BeautifulSoup
from typing import Iterable
import trafilatura

class JSON2ParquetStage(SetuStage):
    """JSON2ParquetStage The SetuStage Class extension for converting jsons to parquet files.

    Args:
        SetuStage (class): SetuStage class to inherit
    """
    def __init__(self, config: Namespace,mode:str=None) -> None:
        """__init__ Initialize the JSON2ParquetStage with the configuration and mode provided

        Args:
            config (Namespace): Configuration Namespace object for the particular language and data source.
            mode (str, optional): Mode that indicates the type of data source to process. Defaults to None.

        Raises:
            ValueError: The mode provided does not correspond to any data source mode.
        """
        super().__init__(config)

        if mode not in ["crawl", "ocr", "asr"]:
            raise ValueError(f"`mode` is given value: {mode}. Only 3 values allowed:{['crawl', 'ocr', 'asr']}")
        else:
            self.mode = mode
            """Mode value indicating the type of data source being processed"""

        
    @staticmethod
    def add_cmdline_args(parser:ArgumentParser)-> ArgumentParser:
            """add_cmdline_args Method that adds the JSON2ParquetStage arguments to the main setu parser.

            Args:
                parser (ArgumentParser): Main Setu parser

            Returns:
                ArgumentParser: Modified Setu parser object with stage arguments
            """
            parser.add_argument(
            "--json_glob_path",
            type=str,
            required=True,
            help="Glob expression to resolve for getting JSON file list that needs to be converted to parquet"
        )

            parser.add_argument(
                "--j2p_cols",
                type=list_of_strings,
                required=False,
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
            spark:SparkSession,
            json_glob_path:str,
            cols:list,
            docs_per_partition:int,
            doc_id_col:str,
            is_multiline:bool,
            output_path:str,
            lang:str) -> None:
        """run_stage_parallelized Method for executing the stage in stage parallel mode.

        Args:
            spark (SparkSession): The current Spark session object.
            json_glob_path (str): The input jsons path.
            cols (list): The list of columns to consider.
            docs_per_partition (int): The number of docs per spark partition.
            doc_id_col (str): The column name for the doc identifier.
            is_multiline (bool): Boolean value indicating if the json are multiline jsons.
            output_path (str): The output path to store the parquet files.
            lang (str): The language of the current data source
        """
        json_df = spark.read.format("json").options(multiline=is_multiline, ignoreCorruptFiles=True).schema(self.schema_creator(cols)).load(json_glob_path)
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
    
    def schema_creator(self, cols:list)->StructType:
        """schema_creator Creats the schema for parquet conversion given a list of column names.

        Args:
            cols (list): List of input column names.

        Returns:
            StructType: Pyspark SQL StructType with Parquet Schema.
        """
        schema = []
        for col in cols:
            schema += [StructField(col, StringType(), True)]
        schema = StructType(schema)
        return schema
    
    def convert_crawl_output(self,spark:SparkSession,json_list:list,docs_per_partition:int,output_path:str,lang:str,run_local:bool,is_multiline=None,**kwargs):
        """convert_crawl_output Method that converts the crawl jsons to parquets.

        Args:
            spark (SparkSession): The current Spark session object.
            json_list (list): list of json paths.
            docs_per_partition (int): The number of docs per spark partition.
            output_path (str): The output path to save the parquets.
            lang (str): The language for the input data.
            run_local (bool): Boolean value indicating local or GCP execution.
            is_multiline (_type_, optional): Boolean value indicating if the json are multiline jsons. Defaults to None.
        """
        def read_files_from_list(idx,partition,lang,run_local=True):

            print(f"Starting processing of partition - {idx}")

            if not run_local:
                tmp_list = [f"{i}\n" for i in json_list]
                with open(f"/tmp/json_data_{self.mode}_{idx}.txt", "w") as tmp_f:
                    tmp_f.writelines(tmp_list)
                subprocess.run([
                    f"cat",
                    f"/tmp/json_data_{self.mode}_{idx}.txt",
                    "|",
                    "gsutil", 
                    "-m",
                    "cp",
                    "-I", 
                    f"/tmp/json_data_{self.mode}_{idx}"
                ])
                json_list = glob.glob(f"/tmp/json_data_{self.mode}_{idx}/*.json")

            for row in partition:
                json_path = row["value"]
                print(f"Performing extraction on: {json_path}")
                with open(json_path, "r") as jf:
                    content = json.load(jf)
                out = [content["doc_id"], content["url"], content["source"], lang, content["html"] if "html" in content else content["text"],content["timestamp"]]
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
            StructField("timestamp", StringType(), True),
        ])
        df = spark.createDataFrame(parquet_rdd, schema=result_schema)
        df = self.salting(df, self.n_splits)
        df.write.mode("overwrite") \
            .parquet(output_path)
        
    def convert_ocr_output(
            self,
            spark:SparkSession,
            json_list:list,
            j2p_bucket:str,
            docs_per_partition:int,
            output_path:str,
            lang:str,
            run_local:bool,
            **kwargs
    ):
        """convert_ocr_output Method that converts the crawl jsons to parquets.

        Args:
            spark (SparkSession): The current Spark session object.
            json_list (list): list of json paths.
            j2p_bucket (str): GCP path for storing parquets
            docs_per_partition (int): The number of docs per spark partition.
            output_path (str): The output path to save the parquets.
            lang (str): The language for the input data.
            run_local (bool): Boolean value indicating local or GCP execution.
        """
        def read_files_from_list(idx:int, partition:Iterable, lang:str, run_local:bool=True):

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
        """convert_asr_output Method that converts the ASR output jsons to parquets.

        Raises:
            NotImplementedError: Currently not supported.
        """
        raise NotImplementedError("`convert_asr_output` function has not been implemented for class `JSON2Parquet`")
    
    def run_data_parallelized(self, **kwargs)->Callable:
        """run_data_parallelized Method that triggers data parallel execution based on data source mode.

        Returns:
            Callable: Returns the onvert Callable corresponding to the data source type.
        """
        if self.mode == "crawl":
            return self.convert_crawl_output(**kwargs)
        elif self.mode == "ocr":
            return self.convert_ocr_output(**kwargs)
        elif self.mode == "asr":
            return self.convert_asr_output(**kwargs)
        
    def run_spark(
            self,
            spark:SparkSession,
            json_glob_path:str,
            j2p_cols:list,
            language:str,
            j2p_samples_per_partition:int,
            j2p_run_mode:str,
            j2p_is_multiline:bool,
            j2p_parquet_output_path:str,
            run_local:bool,
            j2p_bucket:str,
            j2p_bucket_prefix:str,
            **kwargs
            )->Callable:
        """run_spark Method which triggers spark execution of the particular stage.

        Args:
            spark (SparkSession): The current spark session object.
            json_glob_path (str): The input jsons path.
            j2p_cols (list): The input data column list.
            language (str): The language of the current data source.
            j2p_samples_per_partition (int): The number of docs per partition
            j2p_run_mode (str): Either stage or data mode.
            j2p_is_multiline (bool): If the input jsons are multiline jsons.
            j2p_parquet_output_path (str): Output path to save parquets.
            run_local (bool): To execute in local or GCP
            j2p_bucket (str): The bucket to save outputs in GCP
            j2p_bucket_prefix (str): The bucket prefix for the GCP Bucket

        Raises:
            Exception: Incorrect Input Mode provided.

        Returns:
            Callable: Returns run Callable based on stage or data mode.
        """
        if j2p_run_mode =="stage":
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
        
    def run(self,spark:SparkSession,**kwargs) -> Callable:
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs)
        
    def run_normal(
        self, 
        json_glob_path:str,
        j2p_cols:list,
        language:str,
        j2p_samples_per_partition:int,
        j2p_run_mode:str,
        j2p_parquet_output_path:str,
        run_local:bool,
        j2p_bucket:str,
        j2p_bucket_prefix:str
    ):
        raise NotImplementedError("`run_normal` function has not been implemented for class `JSON2Parquet`")


class ExtractTextStage(SetuStage):
    """ExtractTextStage The SetuStage Class extension for extracting the text from the text column in the parquet files.

    Args:
        SetuStage (class): SetuStage class to inherit
    """
    def __init__(self, config: Namespace) -> None:
        """__init__ Initialize the ExtractTextStage with the configuration provided

        Args:
            config (Namespace): Configuration Namespace object for the particular language and data source.
        """
        super().__init__(config)

    @staticmethod
    def add_cmdline_args(parser: ArgumentParser):
        """add_cmdline_args Method that adds the ExtractTextStage arguments to the main setu parser.

        Args:
            parser (ArgumentParser): Main Setu parser

        Returns:
            ArgumentParser: Modified Setu parser object with stage arguments
        """
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
    
    def run_stage_parallelized(self,df:DataFrame,docs_per_partition:int,output_path:str) -> None:
        """run_stage_parallelized Method for running the stage in parallel mode.

        Args:
            df (DataFrame): The input dataframe
            docs_per_partition (int): The number of documents per spark partition
            output_path (str): The output path to save the stage parquets.

        Raises:
            NotImplementedError: Error indicating this method has not been implemented yet.
        """
        raise NotImplementedError("`run_stage_parallelized` function has not been implemented for class `TextExtractionStage`")
    
    def run_data_parallelized(
        self,
        spark:SparkSession,
        df:DataFrame,
        docs_per_partition:int,
        output_path:str
    ):
        """run_data_parallelized Method for running the stage in data parallel mode.

        Args:
            df (DataFrame): The input dataframe
            docs_per_partition (int): The number of documents per spark partition
            output_path (str): The output path to save the stage parquets.
        """
        def extract_text_trafilatura(idx:int, partition:Iterable):
            """extract_text_trafilatura Extract the text from HTML text from the dataframe in the particular partition based on idx.

            Args:
                idx (int): Partition ID
                partition (Iterable): Partition Object

            Yields:
                list: List containint the output result and columns/value pairs of the current partition.
            """
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
        spark:SparkSession,
        te_parquets_path:str,
        te_samples_per_partition:int,
        te_run_mode:str,
        te_output_path:str,
        run_local:bool,
        **kwargs
    )->Callable:
        """run_spark Method which triggers spark execution of the particular stage.

        Args:
            spark (SparkSession): The current Spark session object.
            te_parquets_path (str): The input parquets path.
            te_samples_per_partition (int): The number of samples per spark partition.
            te_run_mode (str): To run in data or stage mode.
            te_output_path (str): The output path to save the stage output parquets.
            run_local (bool): To run on local or GCP.

        Raises:
            Exception: Exception indicating wrong mode provided.

        Returns:
            Callable: Either stage or data parallel run Callable is executed and returned.
        """
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


    def run(self, spark:SparkSession, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs) 

    def run_normal(
        self,
        te_parquets_path:str,
        te_samples_per_partition:int,
        te_run_mode:str,
        te_output_path:str,
        run_local:bool
    ):
        raise NotImplementedError("`run_normal` function has not been implemented for class `TextExtractionStage`")

class TextExtractionComponent(SetuComponent):
    """TextExtractionComponent The SetuComponent Class extension for converting jsons to parquet files and extracting the text from them.

    Args:
        SetuComponent (class): The SetuComponent to inherit.
    """
    def __init__(self, config:Namespace, mode="crawl")->None:
        super().__init__(config)
        self.j2p_stage = JSON2ParquetStage(self.config, mode)
        self.te_stage = ExtractTextStage(self.config)

        self.stages = {
            self.j2p_stage.name: self.j2p_stage,
            self.te_stage.name: self.te_stage,
        }

    @classmethod
    def add_cmdline_args(cls,parser: ArgumentParser,for_full_pipeline=False) -> ArgumentParser:
        if for_full_pipeline:
            parser = JSON2ParquetStage.add_cmdline_args(parser)
            parser = ExtractTextStage.add_cmdline_args(parser)
        else:
            j2p_subparser = parser.add_parser(JSON2ParquetStage.get_stage_name(), help='Run json 2 parquet stage of text extraction component')
            j2p_subparser = JSON2ParquetStage.add_cmdline_args(j2p_subparser)

            te_core_subparser = parser.add_parser(ExtractTextStage.get_stage_name(), help='Run core text extraction stage of text extraction component')
            te_core_subparser = ExtractTextStage.add_cmdline_args(te_core_subparser)

            te_parser = parser.add_parser(cls.get_component_name(), help='Run entire text-extraction component')
            te_parser = JSON2ParquetStage.add_cmdline_args(te_parser)
            te_parser = ExtractTextStage.add_cmdline_args(te_parser)
        return parser
    
    @classmethod
    def get_stage_mapping(cls):
        return dict.fromkeys([
            JSON2ParquetStage.get_stage_name(), 
            ExtractTextStage.get_stage_name(), 
            cls.get_component_name()
        ], cls.get_component_name())

    def run_stage(self, spark, stage, **kwargs):
        return self.stages[stage].run(spark, **kwargs)

    def run(self, spark, stage, **kwargs):
        if stage == self.name:
            for substage in ["JSON2ParquetStage","ExtractTextStage"]:
                self.run_stage(spark=spark,stage=substage,**kwargs)
        else:
            return self.run_stage(spark=spark, stage=stage, **kwargs)