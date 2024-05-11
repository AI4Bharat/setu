import argparse
from base import SetuComponent, SetuStage
from pyspark.sql import SparkSession, DataFrame
from ocr_filters import (
    get_max_lang,
    approx_intersection_cleaner,
    check_script_coverage,
    check_plane_coverage,
    check_bbox_overlap,
    check_block_density,
)

from pyspark.sql.functions import explode, col, max, when, udf
from pyspark.sql.types import (
    Row,
    IntegerType,
    ArrayType,
    BooleanType,
    StringType,
    FloatType,
    StructType,
    StructField,
)
from functools import partial
from utilities import str2bool

get_max_lang_udf = udf(
    get_max_lang,
    StructType(
        [
            StructField("languageCode", StringType(), True),
            StructField("lang_confidence", FloatType(), True),
        ]
    ),
)

approx_intersection_cleaner_udf = udf(
    partial(approx_intersection_cleaner, for_spark=True),
    StructType(
        [
            StructField("bbox_to_keep", ArrayType(IntegerType()), True),
            StructField("bbox_to_keep_count", IntegerType(), True),
            StructField("total_bbox_count", IntegerType(), True),
        ]
    ),
)
check_horizontal_coverage_udf = udf(
    partial(check_plane_coverage, type="horizontal"),
    FloatType(),
)
check_vertical_coverage_udf = udf(
    partial(check_plane_coverage, type="horizontal"),
    FloatType(),
)
check_bbox_overlap_udf = udf(check_bbox_overlap, FloatType())
check_script_coverage_udf = udf(
    partial(check_script_coverage, to_fail_threshold=0.5, failed_paras_ratio=0.3),
    BooleanType(),
)
check_block_density_udf = udf(check_block_density, FloatType())


class FormatConversionStage(SetuStage):
    """FormatConversionStage The SetuStage Class extension for converting GCP OCR JSON data into parquet dataset format.

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
        """add_cmdline_args Method that adds the FlaggingAndFilteringStage arguments to the main setu parser.

        Args:
            parser (ArgumentParser): Main Setu parser

        Returns:
            ArgumentParser: Modified Setu parser object with stage arguments
        """
        parser.add_argument(
            "--fc_json_glob",
            type=str,
            help="Path of input JSONs for ocr postprocessing",
        )

        parser.add_argument(
            "--fc_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--fc_run_mode",
            type=str,
            required=False,
            choices=["stage"],
            default="stage",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--fc_output_path",
            type=str,
            help="Directory where all the formated OCR parquets will be stored.",
        )

        return parser

    def run_stage_parallelized(
        self,
        df: DataFrame,
        docs_per_partition,
        output_path,
    ):
        """run_stage_parallelized Runs a stage in parallelized mode.

        Args:
            df (DataFrame): The DataFrame containing the data to be processed.
            docs_per_partition (int): Number of documents per partition for processing.
            output_path (str): The output path for the analysis results.
        """

        df = (
            df.select("inputConfig.*", "responses")
            .select("gcsSource.*", "mimeType", "responses")
            .select("uri", "mimeType", explode("responses").alias("pages"))
            .select("uri", "mimeType", "pages.*")
            .na.drop(subset=["fullTextAnnotation"])
            .select("uri", "mimeType", "context.pageNumber", "fullTextAnnotation")
            .select("uri", "mimeType", "pageNumber", "fullTextAnnotation.pages")
            .select("uri", "mimeType", "pageNumber", explode("pages").alias("pages"))
            .select("uri", "mimeType", "pageNumber", "pages.*")
            .select(
                "uri",
                "mimeType",
                "pageNumber",
                "blocks",
                "confidence",
                "width",
                "height",
                "property.detectedLanguages",
            )
            .select(
                "uri",
                "mimeType",
                "pageNumber",
                "blocks",
                "confidence",
                "width",
                "height",
                get_max_lang_udf("detectedLanguages").alias("language_scores"),
            )
            .withColumn("page_confidence", col("confidence"))
            .drop("confidence")
            .select(
                "uri",
                "mimeType",
                "pageNumber",
                "blocks",
                "page_confidence",
                "width",
                "height",
                "language_scores.*",
            )
        )

        df.show(5)

        df = self.set_split_count_and_salt(df, docs_per_partition)

        df.write.mode("overwrite").parquet(output_path)

    def run_data_parallelized(
        self,
        spark: SparkSession,
        json_glob: str,
        docs_per_partition: int,
        output_path: str,
    ):
        """run_data_parallelized Runs a stage in data parallelized mode.

        Args:
            spark (SparkSession): The Spark session.
            json_glob (str): The input path for the location of GCP OCR json files.
            docs_per_partition (int): Number of documents per partition for processing.
            output_path (str): The output path for the analysis results.
        Raises:
            NotImplementedError: Error indicating data parallel execution has not been implemented.
        """
        raise NotImplementedError(
            f"`run_data_parallelized` function has not been implemented for class `{self.name}`"
        )

    def run_spark(
        self,
        spark,
        fc_json_glob,
        fc_run_mode,
        fc_samples_per_partition,
        fc_output_path,
        run_local,
    ):
        df = spark.read.format("json").options(multiline=True).load(fc_json_glob)
        if fc_run_mode == "stage":
            return self.run_stage_parallelized(
                df=df,
                docs_per_partition=fc_samples_per_partition,
                output_path=fc_output_path,
            )
        else:
            raise Exception(
                "Incorrect input for `fc_run_mode`. `fc_run_mode` only supports 1 types: `stage`"
            )

    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs)

    def run_normal(
        self,
        fc_json_glob,
        fc_run_mode,
        fc_samples_per_partition,
        fc_output_path,
        run_local,
    ):
        raise NotImplementedError(
            f"`run_normal` function has not been implemented for class `{self.name}`"
        )


class PageAnalysisStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--pa_parquets_path",
            type=str,
            help="Path of parquets for page analysis in OCR postprocessing",
        )

        parser.add_argument(
            "--pa_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--pa_run_mode",
            type=str,
            required=False,
            choices=["stage"],
            default="stage",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--pa_output_path",
            type=str,
            help="Directory where all the analysised pages will be stored.",
        )

        return parser

    def run_stage_parallelized(
        self,
        df,
        docs_per_partition,
        output_path,
    ):
        df = self.set_split_count_and_salt(df, docs_per_partition)
        before_cols = df.columns
        df = (
            df.select(
                "*",
                approx_intersection_cleaner_udf("blocks").alias("intersection_cleaner"),
            )
            .select(*before_cols, "intersection_cleaner.*")
            .select(
                "*",
                check_horizontal_coverage_udf("blocks", "bbox_to_keep").alias(
                    "horizontal_coverage"
                ),
            )
            .select(
                "*",
                check_vertical_coverage_udf("blocks", "bbox_to_keep").alias(
                    "vertical_coverage"
                ),
            )
            .select(
                "*", check_bbox_overlap_udf("blocks", "bbox_to_keep").alias("max_iou")
            )
            .select("*", check_script_coverage_udf("blocks").alias("script_coverage"))
            .select(
                "*",
                check_block_density_udf("blocks", "bbox_to_keep").alias(
                    "min_block_density"
                ),
            )
        )
        df = self.salting(df, self.n_splits)
        df.write.mode("overwrite").parquet(output_path)

    def run_data_parallelized(
        self,
        spark,
        df,
        docs_per_partition,
        output_path,
    ):
        raise NotImplementedError(
            f"`run_data_parallelized` function has not been implemented for class `{self.name}`"
        )

    def run_spark(
        self,
        spark,
        pa_parquets_path,
        pa_run_mode,
        pa_samples_per_partition,
        pa_output_path,
        run_local,
    ):

        df = spark.read.format("parquet").load(pa_parquets_path)

        if pa_run_mode == "stage":
            return self.run_stage_parallelized(
                df=df,
                docs_per_partition=pa_samples_per_partition,
                output_path=pa_output_path,
            )
        else:
            raise Exception(
                "Incorrect input for `pa_run_mode`. `pa_run_mode` only supports 1 types: `stage`"
            )

    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs)

    def run_normal(
        self,
        pa_parquets_path,
        pa_run_mode,
        pa_samples_per_partition,
        pa_output_path,
        run_local,
    ):
        raise NotImplementedError(
            f"`run_normal` function has not been implemented for class `{self.name}`"
        )
    

class PageFilteringStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--pf_parquets_path",
            type=str,
            help="Path of parquets for page analysis in OCR postprocessing"
        )

        parser.add_argument(
            "--pf_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--pf_run_mode",
            type=str,
            required=False,
            choices=["stage"],
            default="stage",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--pf_perform_flagging",
            type=str2bool,
            required=False,
            default=True,
            help="Perform Page Flagging?",
        )

        parser.add_argument(
            "--pf_perform_removal",
            type=str2bool,
            required=False,
            default=True,
            help="Perform Page Removal?",
        )

        parser.add_argument(
            "--pf_output_path",
            type=str,
            help="Directory where all the analysised pages will be stored."
        )
        
        return parser

    def run_stage_parallelized(
        self,
        df,
        docs_per_partition,
        perform_flagging,
        perform_removal,
        output_path,

    ):
        df = self.set_split_count_and_salt(df, docs_per_partition)

        if perform_flagging:
            df = df \
                .select("*", when(df["horizontal_coverage"] < self.config.horizontal_coverage_threshold, True).otherwise(False).alias("is_horizontally_sparse")) \
                .select("*", when(df["vertical_coverage"] < self.config.vertical_coverage_threshold, True).otherwise(False).alias("is_vertically_sparse")) \
                .select("*", when(df["max_iou"] > self.config.max_iou_threshold , True).otherwise(False).alias("is_overlapping")) \
                .select("*", when(df["script_coverage"] == False, True).otherwise(False).alias("is_script_unconfident")) \
                .select("*", when(df["min_block_density"] < self.config.block_density_threshold, True).otherwise(False).alias("contains_sparse_blocks"))

        if perform_removal:
            if self.config.remove_horizontally_sparse:
                df = df.filter(df.is_horizontally_sparse == False)
            if self.config.remove_vertically_sparse:
                df = df.filter(df.is_vertically_sparse == False)
            if self.config.remove_overlapping:
                df = df.filter(df.is_overlapping == False)
            if self.config.remove_script_unconfident:
                df = df.filter(df.is_script_unconfident == False)
            if self.config.remove_sparse_blocked:
                df = df.filter(df.contains_sparse_blocks == False)

        df = self.salting(df, self.n_splits)
        df.write.mode("overwrite").parquet(output_path)

    def run_data_parallelized(
        self,
        spark,
        df,
        docs_per_partition,
        perform_flagging,
        perform_removal,
        output_path,
    ):
        raise NotImplementedError(f"`run_data_parallelized` function has not been implemented for class `{self.name}`")

    def run_spark(
        self,
        spark,
        pf_parquets_path,
        pf_run_mode,
        pf_samples_per_partition,
        pf_perform_flagging,
        pf_perform_removal,
        pf_output_path,
        run_local,
    ):

        df = spark.read.format("parquet").load(pf_parquets_path)

        if pf_run_mode == "stage":
            return self.run_stage_parallelized(
                df=df,
                docs_per_partition=pf_samples_per_partition,
                perform_flagging=pf_perform_flagging,
                perform_removal=pf_perform_removal,
                output_path=pf_output_path,
            )
        else:
            raise Exception("Incorrect input for `pf_run_mode`. `pf_run_mode` only supports 1 types: `stage`.")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs) 

    def run_normal(
        self,
        pf_parquets_path,
        pf_run_mode,
        pf_samples_per_partition,
        pf_perform_flagging,
        pf_perform_removal,
        pf_output_path,
        run_local,
    ):
        raise NotImplementedError(f"`run_normal` function has not been implemented for class `{self.name}`")


class OCRProcessingComponent(SetuComponent):

    def __init__(self, config, mode="crawl"):
        super().__init__(config)
        self.fc_stage = FormatConversionStage(self.config)
        self.pa_stage = PageAnalysisStage(self.config)
        self.pf_stage = PageFilteringStage(self.config)

        self.stages = {
            self.fc_stage.name: self.fc_stage,
            self.pa_stage.name: self.pa_stage,
            self.pf_stage.name: self.pf_stage
        }

    @classmethod
    def add_cmdline_args(cls, parser, for_full_pipeline=False):

        if for_full_pipeline:
            parser = FormatConversionStage.add_cmdline_args(parser)
            parser = PageAnalysisStage.add_cmdline_args(parser)
            parser = PageFilteringStage.add_cmdline_args(parser)

        else:
            fc_subparser = parser.add_parser(
                FormatConversionStage.get_stage_name(),
                help="Run format conversion stage of ocr postprocessing component",
            )
            fc_subparser = FormatConversionStage.add_cmdline_args(fc_subparser)

            pa_subparser = parser.add_parser(
                PageAnalysisStage.get_stage_name(),
                help="Run page analysis stage of ocr postprocessing component",
            )
            pa_subparser = PageAnalysisStage.add_cmdline_args(pa_subparser)

            pf_subparser = parser.add_parser(PageFilteringStage.get_stage_name(), help='Run page filtering stage of ocr postprocessing component')
            pf_subparser = PageFilteringStage.add_cmdline_args(pf_subparser)

            ocr_parser = parser.add_parser(
                cls.get_component_name(), help="Run entire ocr postprocessing component"
            )
            ocr_parser = FormatConversionStage.add_cmdline_args(ocr_parser)
            ocr_parser = PageAnalysisStage.add_cmdline_args(ocr_parser)
            ocr_parser = PageFilteringStage.add_cmdline_args(ocr_parser)

        return parser

    @classmethod
    def get_stage_mapping(cls):
        return dict.fromkeys(
            [
                FormatConversionStage.get_stage_name(),
                PageAnalysisStage.get_stage_name(),
                PageFilteringStage.get_stage_name(),
                cls.get_component_name(),
            ],
            cls.get_component_name(),
        )

    def run_stage(self, spark, stage, **kwargs):
        return self.stages[stage].run(spark, **kwargs)

    def run(self, spark, stage, **kwargs):
        if stage == self.name:
            raise NotImplementedError(
                f"`run` function for running the entire component has not been implemented for class `{self.name}`"
            )
        else:
            return self.run_stage(spark=spark, stage=stage, **kwargs)
