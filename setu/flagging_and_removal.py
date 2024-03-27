import argparse
from base import SetuStage,SetuComponent
from utilities import str2bool,SparkOptimizedHandlers
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf
from filters import has_repetition
from functools import partial
import subprocess
from pyspark.sql import DataFrame,SparkSession
from typing import Iterable,Callable

class FlaggingAndFilteringStage(SetuStage):
    """FlaggingAndFilteringStage The SetuStage Class extension for flagging and filtering documents.

    Args:
        SetuStage (class): SetuStage class to inherit
    """
    def __init__(self, config):
        """__init__ Initialize the ExtractTextStage with the configuration provided

        Args:
            config (Namespace): Configuration Namespace object for the particular language and data source.
        """
        super().__init__(config)

        if self.spark_present:
            self.spark_optimized_handler = SparkOptimizedHandlers()
            """The SparkOptimizedHandlers for handling document stat generation"""
            self.wrap_funcs_with_udf()

    def wrap_funcs_with_udf(self):
        """wrap_funcs_with_udf Method that wraps the functions as Spark UDFs.
        """
        self.has_char_repetition_udf = udf(
            partial(
                has_repetition, 
                repetition_thresholds=self.config.char_ngram_cum_thresholds
            ),
            BooleanType()
        ) # Doc level
        self.has_word_repetition_udf = udf(
            partial(
                has_repetition, 
                repetition_thresholds=self.config.word_ngram_cum_thresholds
            ),  
            BooleanType()
        ) # Doc level

    @staticmethod
    def add_cmdline_args(parser):
        """add_cmdline_args Method that adds the FlaggingAndFilteringStage arguments to the main setu parser.

        Args:
            parser (ArgumentParser): Main Setu parser

        Returns:
            ArgumentParser: Modified Setu parser object with stage arguments
        """
        parser.add_argument(
            "--doc_stats_parquets_path",
            type=str,
            help="Path to folder containing parquets",
        )

        parser.add_argument(
            "--is_doc_stats_path_batched",
            type=str2bool,
            required=False,
            default=True,
            help="Is path a batch path or not?",
        )

        parser.add_argument(
            "--fnf_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--fnf_verbose",
            type=str2bool,
            default=False,
            required=False,
            help="Whether to add `show()`",
        )

        parser.add_argument(
            "--fnf_run_mode",
            type=str,
            required=False,
            choices=["stage"],
            default="stage",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--save_nsfw_data",
            type=str2bool,
            default=True,
            required=False,
            help="Whether to store nsfw data",
        )

        parser.add_argument(
            "--nsfw_output_path",
            type=str,
            help="Path of the folder to store nsfw data",
        )

        parser.add_argument(
            "--filtered_doc_stats_output_path",
            type=str,
            help="Path to the folder to store fitered output",
        )

        return parser

    def run_stage_parallelized(
        self,
        doc_stats_df:DataFrame,
        docs_per_partition:int,
        save_nsfw_data:bool,
        nsfw_output_path:str,
        filtered_doc_stats_output_path:str,
        verbose:bool = True,
    )->None:
        """run_stage_parallelized Runs a stage in parallelized mode.

        Args:
            doc_stats_df (DataFrame): The DataFrame containing the data to be processed.
            docs_per_partition (int): Number of documents per partition for processing.
            save_nsfw_data (bool): Whether to save the nsfw data found.
            nsfw_output_path (str): The output path for nsfw results.
            filtered_doc_stats_output_path (str): The output path for filtered documents.
            verbose (bool, optional): Whether to display verbose output. Defaults to True.

        Raises:
            Exception: Exception indicating to pass both `doc_stats_df` and `df` when `run_flagging`
        """
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

        if self.config.char_repetition_filter:
            doc_stats_df = doc_stats_df.select("*", self.has_char_repetition_udf("char_ngram_repetition_score").alias("has_char_repetition")) \
                                        .drop("char_ngram_repetition_score")

        if self.config.word_repetition_filter:
            doc_stats_df = doc_stats_df.select("*", self.has_word_repetition_udf("word_ngram_repetition_score").alias("has_word_repetition")) \
                                        .drop("word_ngram_repetition_score")

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
                nsfw_stats_df = doc_stats_df.filter(doc_stats_df.is_nsfw_heavy == True)
                nsfw_stats_df.write.mode("overwrite") \
                                .parquet(nsfw_output_path if nsfw_output_path else self.config.nsfw_output_path)
                nsfw_stats_df.show(5)
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


    def run_data_parallelized(
        self, 
        doc_stats_df:DataFrame,
        docs_per_partition:int,
        save_nsfw_data:bool,
        nsfw_output_path:str,
        filtered_doc_stats_output_path:str,
        verbose:bool = True,
    ):
        """run_data_parallelized Runs a stage in data parallelized mode.

        Args:
            doc_stats_df (DataFrame): The DataFrame containing the data to be processed.
            docs_per_partition (int): Number of documents per partition for processing.
            save_nsfw_data (bool): Whether to save the nsfw data found.
            nsfw_output_path (str): The output path for nsfw results.
            filtered_doc_stats_output_path (str): The output path for filtered documents.
            verbose (bool, optional): Whether to display verbose output. Defaults to True.

        Raises:
            NotImplementedError: Error indicating data parallel execution has not been implemented.
        """
        raise NotImplementedError("`run_data_parallelized` function has not been implemented for class `FlaggingAndFilteringStage`")

    def run_spark(
        self,
        spark:SparkSession,
        doc_stats_parquets_path:str,
        is_doc_stats_path_batched:bool,
        fnf_samples_per_partition:int,
        fnf_verbose:bool,
        fnf_run_mode:str,
        save_nsfw_data:bool,
        nsfw_output_path:str,
        filtered_doc_stats_output_path:str,
        run_local:bool,
    )->Callable:
        """run_spark Run Spark processing.

        Args:
            spark (SparkSession): The Spark session.
            doc_df_parquets_path (str): Path to the parquet files containing document data.
            is_doc_df_path_batched (bool): Flag indicating whether the document DataFrame path is batched.
            fnf_samples_per_partition (int): Number of document samples per partition.
            fnf_verbose (bool): Flag indicating whether to display verbose output.
            fnf_run_mode (str): The run mode for flagging and filtering.
            save_nsfw_data (bool): Whether to save nsfw results.
            nsfw_output_path (str): The output path for nsfw results.
            filtered_doc_stats_output_path (str): The output path for filtered documents.
            run_local (bool): Flag indicating whether to run locally.

        Raises:
            Exception: Exception indicating the run_mode value is unknown.

        Returns:
            Callable: The execution function based on the run_mode.
        """
        # if is_doc_stats_path_batched:
        #     if not run_local:
        #         pass
        #         subprocess.run([[
        #             "gsutil",
        #             "cp",
        #             doc_stats_parquets_path,
        #             "/tmp/fnf_batch.info"
        #         ]])
        #     with open("/tmp/fnf_batch.info", "r") as batch_f:
        #         parquet_list = [line.strip() for line in batch_f.readlines()]
        #     doc_stats_df = spark.read.format("parquet").load(parquet_list)
        # else:
        doc_stats_df = spark.read.format("parquet").load(doc_stats_parquets_path)

        doc_stats_df = doc_stats_df.dropDuplicates(["doc_id"])

        if fnf_run_mode == "stage":
            return self.run_stage_parallelized(
                doc_stats_df=doc_stats_df,
                docs_per_partition=fnf_samples_per_partition,
                save_nsfw_data=save_nsfw_data,
                nsfw_output_path=nsfw_output_path,
                filtered_doc_stats_output_path=filtered_doc_stats_output_path,
                verbose=fnf_verbose,
            )
        elif fnf_run_mode == "data":
            return self.run_data_parallelized(
                spark=spark,
                doc_stats_df=doc_stats_df,
                docs_per_partition=fnf_samples_per_partition,
                save_nsfw_data=save_nsfw_data,
                nsfw_output_path=nsfw_output_path,
                filtered_doc_stats_output_path=filtered_doc_stats_output_path,
                verbose=fnf_verbose,
            )
        else:
            raise Exception("Incorrect input for `fnf_run_mode`. `fnf_run_mode` only supports 2 types: `stage` & `data`.")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs) 

    def run_normal(
        self, 
        doc_stats_parquets_path,
        is_doc_stats_path_batched,
        fnf_samples_per_partition,
        fnf_verbose,
        fnf_run_mode,
        save_nsfw_data,
        nsfw_output_path,
        filtered_doc_stats_output_path,
        run_local
    ):
        raise NotImplementedError("`run_normal` function has not been implemented for class `FlaggingAndFilteringStage`")
    
class DocumentRemovalStage(SetuStage):
    """DocumentRemovalStage The SetuStage Class extension for document removal.

    Args:
        SetuStage (class): SetuStage class to inherit
    """
    def __init__(self, config):
        """__init__ Initialize the ExtractTextStage with the configuration provided

        Args:
            config (Namespace): Configuration Namespace object for the particular language and data source.
        """
        super().__init__(config)

    @staticmethod
    def add_cmdline_args(parser):
        """add_cmdline_args Method that adds the ExtractTextStage arguments to the main setu parser.

        Args:
            parser (ArgumentParser): Main Setu parser

        Returns:
            ArgumentParser: Modified Setu parser object with stage arguments
        """
        parser.add_argument(
            "--analysis_out_path",
            type=str,
            help="Path to the analysis output on which to perform inner join",
        )

        parser.add_argument(
            "--doc_stats_path",
            type=str,
            help="Path to the filtered doc stats used to perform inner join",
        )

        parser.add_argument(
            "--doc_removal_join_col",
            type=str,
            help="Col to use for joining",
        )

        parser.add_argument(
            "--doc_removal_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--doc_removal_verbose",
            type=str2bool,
            default=False,
            required=False,
            help="Whether to add `show()`",
        )

        parser.add_argument(
            "--doc_removal_run_mode",
            type=str,
            required=False,
            choices=["stage"],
            default="stage",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--filtered_docs_path",
            type=str,
            help="Path to the folder to store fitered output",
        )

        return parser

    def run_stage_parallelized(
        self,
        df:DataFrame,
        doc_stats_df:DataFrame,
        docs_per_partition:int,
        doc_id_col:str,
        filtered_docs_path:str,
    )->None:
        """Run a stage of processing in parallel.

        Args:
            df (DataFrame): The DataFrame containing data to be processed.
            doc_stats_df (DataFrame): The DataFrame containing document statistics.
            docs_per_partition (int): The number of documents per partition for processing.
            doc_id_col (str): The column name representing document IDs.
            filtered_docs_path (str): The path to save filtered documents.

        Returns:
            None
        """
        print("Starting SETU Document Removal Spark Pipeline...........")
        
        df = self.set_split_count_and_salt(df, docs_per_partition)
        doc_stats_df = self.salting(doc_stats_df, self.n_splits)

        df = df.join(doc_stats_df.drop("doc_lang"), [doc_id_col], "inner")

        df.write.mode("overwrite") \
                .parquet(filtered_docs_path)

        print(f"Completed `filtered_docs` parquet write...written to: {filtered_docs_path}")

    def run_data_parallelized(
            self,
            df: DataFrame,
            doc_stats_df: DataFrame,
            docs_per_partition: int,
            doc_id_col: str,
            filtered_docs_path: str,
        ) -> None:
        """Run a parallelized data processing operation.

        This function is not implemented and raises a NotImplementedError.

        Args:
            df (DataFrame): The DataFrame containing data to be processed.
            doc_stats_df (DataFrame): The DataFrame containing document statistics.
            docs_per_partition (int): The number of documents per partition for processing.
            doc_id_col (str): The column name representing document IDs.
            filtered_docs_path (str): The path to save filtered documents.

        Raises:
            NotImplementedError: This function has not been implemented.

        Returns:
            None
        """
        raise NotImplementedError("`run_data_parallelized` function has not been implemented for class `DocumentRemovalStage`")

    def run_spark(
        self,
        spark,
        analysis_out_path,
        doc_stats_path,
        doc_removal_join_col,
        doc_removal_samples_per_partition,
        doc_removal_verbose,
        doc_removal_run_mode,
        filtered_docs_path,
        run_local,
    ):

        df = spark.read.format("parquet").load(analysis_out_path)
        df = df.dropDuplicates(["doc_id"])
        
        doc_stats_df = spark.read.format("parquet").load(doc_stats_path)

        if doc_removal_run_mode == "stage":
            return self.run_stage_parallelized(
                df=df,
                doc_stats_df=doc_stats_df,
                docs_per_partition=doc_removal_samples_per_partition,
                doc_id_col=doc_removal_join_col,
                filtered_docs_path=filtered_docs_path,
            )
        elif doc_removal_run_mode == "data":
            return self.run_data_parallelized(
                spark=spark,
                df=df,
                doc_stats_df=doc_stats_df,
                docs_per_partition=doc_removal_samples_per_partition,
                doc_id_col=doc_removal_join_col,
                filtered_docs_path=filtered_docs_path,
            )
        else:
            raise Exception("Incorrect input for `doc_removal_run_mode`. `doc_removal_run_mode` only supports 2 types: `stage` & `data`.")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs) 

    def run_normal(
        self, 
        analysis_out_path,
        doc_stats_path,
        doc_removal_join_col,
        doc_removal_samples_per_partition,
        doc_removal_verbose,
        doc_removal_run_mode,
        filtered_docs_path,
        run_local,
    ):
        raise NotImplementedError("`run_normal` function has not been implemented for class `DocumentRemovalStage`")
    
class FlagAndRemoveComponent(SetuComponent):

    def __init__(self, config):
        super().__init__(config)

        self.ff_stage = FlaggingAndFilteringStage(self.config)
        self.doc_removal_stage = DocumentRemovalStage(self.config)

        self.stages = {
            self.ff_stage.name: self.ff_stage,
            self.doc_removal_stage.name: self.doc_removal_stage,
        }

    @classmethod
    def add_cmdline_args(cls, parser, for_full_pipeline=False): 

        if for_full_pipeline:
            parser = FlaggingAndFilteringStage.add_cmdline_args(parser)
            parser = DocumentRemovalStage.add_cmdline_args(parser)
        else:
            fnf_subparser = parser.add_parser(FlaggingAndFilteringStage.get_stage_name(), help='Run flagging and filtering stage of flag-filter component')
            fnf_subparser = FlaggingAndFilteringStage.add_cmdline_args(fnf_subparser)

            dr_subparser = parser.add_parser(DocumentRemovalStage.get_stage_name(), help='Run document removal stage of flag-filter component')
            dr_subparser = DocumentRemovalStage.add_cmdline_args(dr_subparser)

            fnr_parser = parser.add_parser(cls.get_component_name(), help='Run entire flagging and removal component')
            fnr_parser = FlaggingAndFilteringStage.add_cmdline_args(fnr_parser)
            fnr_parser = DocumentRemovalStage.add_cmdline_args(fnr_parser)

        return parser

    @classmethod
    def get_stage_mapping(cls):
        return dict.fromkeys([
            FlaggingAndFilteringStage.get_stage_name(), 
            DocumentRemovalStage.get_stage_name(), 
            cls.get_component_name()
        ], cls.get_component_name())

    def run_stage(self, spark, stage, **kwargs):
        return self.stages[stage].run(spark, **kwargs)

    def run(self, spark, stage, **kwargs):
        if stage == self.name:
            raise NotImplementedError("`run` function for running the entire component has not been implemented for class `FlagAndRemoveComponent`")
        else:
            return self.run_stage(spark=spark, stage=stage, **kwargs)