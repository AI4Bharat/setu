import argparse
import subprocess
from core import SetuStage, str2bool, list_of_strings
from pyspark.sql.types import (
    LongType,
    IntegerType,
    StringType, 
    StructType,
    StructField
)

class CustomJoinStage(SetuStage):
    
    def __init__(self, config):
        super().__init__(config)

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--primary_parquets_path",
            type=str,
            help="Path to the primary parquets",
        )

        parser.add_argument(
            "--columns_from_primary_parquets",
            type=list_of_strings,
            help="`,` separated Columns to select from primary parquets apart from `identifier_cols`",
        )

        parser.add_argument(
            "--secondary_parquets_path",
            type=str,
            help="Path to the secondary parquets",
        )

        parser.add_argument(
            "--columns_from_secondary_parquets",
            type=list_of_strings,
            help="`,` separated Columns to select from secondary parquets apart from `identifier_cols`",
        )

        parser.add_argument(
            "--join_cols",
            type=list_of_strings,
            help="`,` separated Columns to used for groupby",
        )

        parser.add_argument(
            "--identifier_cols",
            type=list_of_strings,
            help="`,` separated Columns to use as identifiers",
        )

        parser.add_argument(
            "--join_type",
            type=str,
            default="inner",
            choices=[
                "inner", "cross", "outer", "full", "fullouter", 
                "full_outer", "left", "leftouter", "left_outer", 
                "right", "rightouter", "right_outer", "semi", 
                "leftsemi", "left_semi", "anti", "leftanti",
                "left_anti"
            ],
            help="Type of join to perform",
        )

        parser.add_argument(
            "--custom_join_run_mode",
            type=str,
            required=False,
            choices=["stage"],
            default="stage",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--joined_output_path",
            type=str,
            help="Path to the folder to store joined output",
        )

        return parser

    def run_stage_parallelized(
        self,
        primary_df,
        columns_from_primary_parquets,
        secondary_df,
        columns_from_secondary_parquets,
        join_cols,
        identifier_cols,
        join_type,
        joined_output_path,
    ):
        print("Starting SETU Custom Join Spark Pipeline...........")

        if columns_from_primary_parquets != "*":
            primary_df = primary_df.select(*columns_from_primary_parquets)
        
        if columns_from_secondary_parquets != "*":
            secondary_df = secondary_df.select(*columns_from_secondary_parquets)

        joined_df = primary_df.join(secondary_df, on=join_cols, how=join_type)

        joined_df = joined_df.dropDuplicates(identifier_cols)

        joined_df.write.mode("overwrite") \
                    .parquet(joined_output_path)

        print(f"Completed `custom_join` parquet write...written to: {joined_output_path}")

    def run_data_parallelized(
        self, 
        **kwargs
    ):
        raise NotImplementedError(f"`run_data_parallelized` function has not been implemented for class `{self.name}`")

    def run_spark(
        self,
        spark,
        primary_parquets_path,
        columns_from_primary_parquets,
        secondary_parquets_path,
        columns_from_secondary_parquets,
        join_cols,
        identifier_cols,
        join_type,
        custom_join_run_mode,
        joined_output_path,
        run_local,
    ):
        primary_df = spark.read.format("parquet").load(primary_parquets_path)
        secondary_df = spark.read.format("parquet").load(secondary_parquets_path)

        if custom_join_run_mode == "stage":
            return self.run_stage_parallelized(
                primary_df=primary_df,
                columns_from_primary_parquets=columns_from_primary_parquets,
                secondary_df=secondary_df,
                columns_from_secondary_parquets=columns_from_secondary_parquets,
                join_cols=join_cols,
                identifier_cols=identifier_cols,
                join_type=join_type,
                joined_output_path=joined_output_path,
            )
        else:
            raise Exception("Incorrect input for `custom_join_run_mode`. `custom_join_run_mode` only supports 1 type: `stage`")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs) 

    def run_normal(
        self, 
        primary_parquets_path,
        columns_from_primary_parquets,
        secondary_parquets_path,
        columns_from_secondary_parquets,
        join_cols,
        identifier_cols,
        join_type,
        custom_join_run_mode,
        joined_output_path,
        run_local,
    ):
        raise NotImplementedError(f"`run_normal` function has not been implemented for class `{self.name}`")