import argparse
from core import SetuStage, str2bool
import subprocess
from .ocr_filters import get_max_lang
from pyspark.sql.functions import explode, col, max, when, udf
from pyspark.sql.types import (
    BooleanType, 
    FloatType,
    StringType, 
    StructType, 
    StructField,
    Row
)
import glob
import trafilatura

get_max_lang_udf = udf(
    get_max_lang,
    StructType([
        StructField("languageCode", StringType(), True),
        StructField("lang_confidence", FloatType(), True),
    ]),
)

class FormatConversionStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--fc_json_glob",
            type=str,
            help="Path of input JSONs for ocr postprocessing"
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
            help="Directory where all the formated OCR parquets will be stored."
        )
        
        return parser

    def run_stage_parallelized(
        self,
        df,
        docs_per_partition,
        output_path,
    ):
        
        # df = df.select("inputConfig.*", "responses") \
        #         .select("gcsSource.*", "mimeType", "responses") \
        #         .select("uri", "mimeType", explode("responses").alias("pages")) \
        #         .select("uri", "mimeType", "pages.*") \
        #         .na.drop(subset=["fullTextAnnotation"]) \
        #         .select("uri", "mimeType", "context.pageNumber", "fullTextAnnotation") \
        #         .select("uri", "mimeType", "pageNumber", "fullTextAnnotation.pages") \
        #         .select("uri", "mimeType", "pageNumber", explode("pages").alias("pages")) \
        #         .select("uri", "mimeType", "pageNumber", "pages.*") \
        #         .select("uri", "mimeType", "pageNumber", "blocks", "confidence", "width", "height", "property.detectedLanguages") \
        #         .select("uri", "mimeType", "pageNumber", "blocks", "confidence", "width", "height", explode("detectedLanguages").alias("language_scores")) \
        #         .withColumn("page_confidence", col("confidence")).drop("confidence") \
        #         .select("uri", "mimeType", "pageNumber", "blocks", "page_confidence", "width", "height", "language_scores.*") \
        #         .withColumn("lang_confidence", col("confidence")).drop("confidence")
                
        # lang_df = df.groupBy("uri", "pageNumber").agg(max("lang_confidence").alias("lang_confidence"))
        # df = df.join(lang_df, ["uri", "pageNumber", "lang_confidence"])
        # df = self.set_split_count_and_salt(df, docs_per_partition)

        df = df.select("inputConfig.*", "responses") \
                .select("gcsSource.*", "mimeType", "responses") \
                .select("uri", "mimeType", explode("responses").alias("pages")) \
                .select("uri", "mimeType", "pages.*") \
                .na.drop(subset=["fullTextAnnotation"]) \
                .select("uri", "mimeType", "context.pageNumber", "fullTextAnnotation") \
                .select("uri", "mimeType", "pageNumber", "fullTextAnnotation.pages") \
                .select("uri", "mimeType", "pageNumber", explode("pages").alias("pages")) \
                .select("uri", "mimeType", "pageNumber", "pages.*") \
                .select("uri", "mimeType", "pageNumber", "blocks", "confidence", "width", "height", "property.detectedLanguages") \
                .select("uri", "mimeType", "pageNumber", "blocks", "confidence", "width", "height", get_max_lang_udf("detectedLanguages").alias("language_scores")) \
                .withColumn("page_confidence", col("confidence")).drop("confidence") \
                .select("uri", "mimeType", "pageNumber", "blocks", "page_confidence", "width", "height", "language_scores.*")

        df.show(5)

        df = self.set_split_count_and_salt(df, docs_per_partition)

        df.write.mode("overwrite").parquet(output_path)


    def run_data_parallelized(
        self,
        spark,
        json_glob,
        docs_per_partition,
        output_path,
    ):
        raise NotImplementedError(f"`run_data_parallelized` function has not been implemented for class `{self.name}`")

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
            raise Exception("Incorrect input for `fc_run_mode`. `fc_run_mode` only supports 1 types: `stage`")


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
        raise NotImplementedError(f"`run_normal` function has not been implemented for class `{self.name}`")