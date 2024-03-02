import argparse
import subprocess
from core import (
    SetuStage,
    str2bool
)
import matplotlib.pyplot as plt
from google.cloud import storage
import pandas as pd
from pyspark.sql.functions import sum, udf, col
from pyspark.sql.types import FloatType
import os

in_gb = udf(lambda x: x/(1024 * 1024 * 1024), FloatType())


class VisualizeAnalysisStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

        if not self.spark_present:
            raise Exception(f"Currently, {self.name} can only run in spark environment")

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--analysis_doc_stats_parquets",
            type=str,
            help="Path to folder containing parquets",
        )

        parser.add_argument(
            "--analysis_viz_output",
            type=str,
            help="Path to folder where viz output will be stored",
        )

        parser.add_argument(
            "--ana_viz_language",
            type=str,
            help="Language of the data",
        )

        parser.add_argument(
            "--ana_viz_gcp_bucket",
            type=str,
            default=None,
            required=False,
            help="Bucket to store data in if not running in locally",
        )

        parser.add_argument(
            "--viz_analysis_stage_run_mode",
            type=str,
            required=False,
            choices=["stage"],
            default="stage",
            help="Which mode to run the stage in.",
        )

        return parser
    
    def save_csv(self, df, base_dir, filename, run_local, bucket):
        if run_local:
            df.to_csv(os.path.join(base_dir, filename))
        else:
            df.to_csv(f"/tmp/{filename}")
            blob = bucket.blob(os.path.join(base_dir.replace(f"gs://{bucket.name}/", ""), filename))
            blob.upload_from_filename(f"/tmp/{filename}")

    def save_png(self, base_dir, filename, run_local, bucket):
        if run_local:
            plt.savefig(os.path.join(base_dir, filename))
        else:
            plt.savefig(f"/tmp/{filename}")
            blob = bucket.blob(os.path.join(base_dir.replace(f"gs://{bucket.name}/", ""), filename))
            blob.upload_from_filename(f"/tmp/{filename}")

    def run_stage_parallelized(
        self,
        doc_stats_df,
        analysis_viz_output,
        ana_viz_language,
        run_local
    ):
        words_count_per_lang_pd = doc_stats_df.groupby("doc_lang").agg(sum("words_count")).sort("doc_lang").toPandas()
        self.save_csv(
            df=words_count_per_lang_pd, 
            base_dir=analysis_viz_output, 
            filename="words_count_per_lang.csv",
            run_local=run_local, 
            bucket=self.ana_viz_gcp_bucket
        )
        bytes_per_lang_pd = doc_stats_df.groupby("doc_lang").agg(sum("bytes")).withColumn("size(in GB)", in_gb("sum(bytes)")).drop("sum(bytes)").sort("doc_lang").toPandas()
        self.save_csv(
            df=bytes_per_lang_pd, 
            base_dir=analysis_viz_output, 
            filename="bytes_per_lang.csv",
            run_local=run_local, 
            bucket=self.ana_viz_gcp_bucket
        )
        docs_count_per_lang_pd = doc_stats_df.groupby("doc_lang").count().sort("doc_lang").toPandas()
        self.save_csv(
            df=docs_count_per_lang_pd, 
            base_dir=analysis_viz_output,
            filename="docs_count_per_lang.csv", 
            run_local=run_local, 
            bucket=self.ana_viz_gcp_bucket
        )

        lines_count_pd = doc_stats_df.filter(doc_stats_df.doc_lang == ana_viz_language).select("lines_count").toPandas()
        self.save_csv(
            df=lines_count_pd, 
            base_dir=analysis_viz_output, 
            filename=f"lines_count_{ana_viz_language}.csv", 
            run_local=run_local, 
            bucket=self.ana_viz_gcp_bucket
        )

        lines_count_pd.hist(bins=100)
        self.save_png(
            base_dir=analysis_viz_output, 
            filename=f"lines_count_{ana_viz_language}_plot.png", 
            run_local=run_local, 
            bucket=self.ana_viz_gcp_bucket
        )

        short_lines_pd = doc_stats_df.filter(doc_stats_df.doc_lang == ana_viz_language).select("mean_line_length").toPandas()
        self.save_csv(
            df=short_lines_pd, 
            base_dir=analysis_viz_output,
            filename=f"short_lines_{ana_viz_language}.csv",
            run_local=run_local,
            bucket=self.ana_viz_gcp_bucket
        )

        short_lines_pd.hist(bins=50)
        self.save_png(
            base_dir=analysis_viz_output, 
            filename=f"short_lines_{ana_viz_language}_plot.png", 
            run_local=run_local,
            bucket=self.ana_viz_gcp_bucket
        )

        non_li_pd = doc_stats_df.filter(doc_stats_df.doc_lang == ana_viz_language).select(col("non_li_char_count")/col("char_count")).toPandas()
        self.save_csv(
            df=non_li_pd, 
            base_dir=analysis_viz_output, 
            filename=f"non_li_{ana_viz_language}.csv", 
            run_local=run_local,
            bucket=self.ana_viz_gcp_bucket
        )
        non_li_pd.hist(bins=50)
        self.save_png(
            base_dir=analysis_viz_output, 
            filename=f"non_li_{ana_viz_language}_plot.png", 
            run_local=run_local, 
            bucket=self.ana_viz_gcp_bucket
        )

        if self.config.calculate_repetition_scores:
            char_10_gram_pd = doc_stats_df.filter(doc_stats_df.doc_lang == ana_viz_language).select("10_gram_characters_repetition_score").toPandas()
            self.save_csv(
                df=char_10_gram_pd, 
                base_dir=analysis_viz_output, 
                filename=f"char_10_gram_{ana_viz_language}.csv", 
                run_local=run_local, 
                bucket=self.ana_viz_gcp_bucket
            )
            char_10_gram_pd.hist(bins=100)
            self.save_png(
                base_dir=analysis_viz_output, 
                filename=f"char_10_gram_{ana_viz_language}_plot.png", 
                run_local=run_local, 
                bucket=self.ana_viz_gcp_bucket
            )

            word_5_gram_pd = doc_stats_df.filter(doc_stats_df.doc_lang == ana_viz_language).select("5_gram_words_repetition_score").toPandas()
            self.save_csv(
                df=word_5_gram_pd, 
                base_dir=analysis_viz_output, 
                filename=f"word_5_gram_{ana_viz_language}.csv", 
                run_local=run_local, 
                bucket=self.ana_viz_gcp_bucket
            )
            word_5_gram_pd.hist(bins=50)
            self.save_png(
                base_dir=analysis_viz_output, 
                filename=f"word_5_gram_{ana_viz_language}_plot.png", 
                run_local=run_local, 
                bucket=self.ana_viz_gcp_bucket
            )

        nsfw_pd = doc_stats_df.filter(doc_stats_df.doc_lang == ana_viz_language).select(col("nsfw_words_count")/col("words_count")).toPandas()
        self.save_csv(
            df=nsfw_pd, 
            base_dir=analysis_viz_output, 
            filename=f"nsfw_{ana_viz_language}.csv", 
            run_local=run_local, 
            bucket=self.ana_viz_gcp_bucket
        )
        nsfw_pd.hist(bins=50)
        self.save_png(
            base_dir=analysis_viz_output, 
            filename=f"nsfw_{ana_viz_language}_plot.png", 
            run_local=run_local, 
            bucket=self.ana_viz_gcp_bucket
        )


    def run_data_parallelized(
        self,
        spark,
        doc_stats_df,
        analysis_viz_output,
        ana_viz_language,
        viz_analysis_stage_run_mode,
        run_local,
    ):
        raise NotImplementedError("`run_data_parallelized` function has not been implemented for class `VisualizeAnalysisStage`")

    def run_spark(
        self,
        spark,
        analysis_doc_stats_parquets,
        analysis_viz_output,
        ana_viz_language,
        ana_viz_gcp_bucket,
        viz_analysis_stage_run_mode,
        run_local,
    ):

        if not run_local:
            self.client = storage.Client()
            self.ana_viz_gcp_bucket = self.client.bucket(ana_viz_gcp_bucket)

        doc_stats_df = spark.read.format("parquet").options(inferSchema="true").parquet(analysis_doc_stats_parquets)

        if viz_analysis_stage_run_mode == "stage":
            return self.run_stage_parallelized(
                doc_stats_df=doc_stats_df,
                analysis_viz_output=analysis_viz_output,
                ana_viz_language=ana_viz_language,
                run_local=run_local,
            )
        elif viz_analysis_stage_run_mode == "data":
            return self.run_data_parallelized(
                spark=spark,
                doc_stats_df=doc_stats_df,
                analysis_viz_output=analysis_viz_output,
                ana_viz_language=ana_viz_language,
                run_local=run_local,
            )
        else:
            raise Exception("Incorrect input for `viz_analysis_stage_run_mode`. `viz_analysis_stage_run_mode` only supports 2 types: `stage` & `data`.")


    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs)

    def run_normal(
        self,
        analysis_doc_stats_parquets,
        analysis_viz_output,
        ana_viz_language,
        ana_viz_gcp_bucket,
        viz_analysis_stage_run_mode,
        run_local,
    ):
        raise NotImplementedError("`run_normal` function has not been implemented for class `VisualizeAnalysisStage`")