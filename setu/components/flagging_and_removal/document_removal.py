import argparse
import subprocess
from core import SetuStage, str2bool

class DocumentRemovalStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

    @staticmethod
    def add_cmdline_args(parser):

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

    def run_data_parallelized(
        self, 
        df,
        doc_stats_df,
        docs_per_partition,
        doc_id_col,
        filtered_docs_path
    ):
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