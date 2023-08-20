import argparse
from setu import Setu
from pyspark.sql import SparkSession
import threading
import traceback
from parse_args import parse_args

if __name__ == "__main__":

    args = parse_args()

    print("Entered Commandline Arguments: ", args)

    setu = Setu(config_file=args.config)

    spark = SparkSession \
                .builder \
                .appName(setu.config.appname) \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.sql.broadcastTimeout", "36000") \
                .config("spark.driver.maxResultSize", "0") \
                .getOrCreate()

    # spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 104857600)
        
    spark_context = spark.sparkContext

    spark.sparkContext.setCheckpointDir(args.checkpoint_dir)
    
    try:

        if args.run_analysis:

            if args.is_df_path_batched:
                parquet_list = args.df_parquets_path.strip().split(",")
                df = spark.read.parquet(*parquet_list)
            else:
                df = spark.read.parquet(args.df_parquets_path)

            setu.run_analysis_spark_pipeline(
                df,
                cols_to_use=[
                    "doc_id", "url", "source", "text", 
                    "language", "successful_extraction"
                ],
                doc_id_col="doc_id",
                text_col="text",
                docs_per_partition=args.samples_per_partition,
                save_doc_lid_output=args.save_doc_lid_output,
                doc_lid_output_path=args.doc_lid_output_path,
                save_line_stats_output=args.save_line_stats_output,
                line_stats_output_path=args.line_stats_output_path,
                save_doc_stats_output=args.save_doc_stats_output,
                doc_stats_output_path=args.doc_stats_output_path,
                analysis_output_path=args.analysis_output_path,
                verbose=args.verbose,
            )

        if args.run_flag_and_filter:

            if args.is_doc_stats_path_batched:
                parquet_list = args.doc_stats_parquets_path.strip().split(",")
                doc_stats_df = spark.read.parquet(*parquet_list)
            else:
                doc_stats_df = spark.read.parquet(args.doc_stats_parquets_path)

            setu.run_flagging_and_filtering_spark_pipeline(
                doc_stats_df,
                docs_per_partition=args.samples_per_partition,
                save_nsfw_data=args.save_nsfw_data,
                nsfw_output_path=args.nsfw_output_path,
                filtered_doc_stats_output_path=args.filtered_doc_stats_output_path,
                verbose=args.verbose,
            )

        if args.remove_documents:

            if args.is_doc_stats_path_batched:
                raise Exception("For document removal stage - `is_doc_stats_path_batched` cannot be set to true.")

            if args.is_df_path_batched:
                parquet_list = args.df_parquets_path.strip().split(","),
                df = spark.read.parquet(*parquet_list)
            else:
                df = spark.read.parquet(args.df_parquets_path)

            doc_stats_df = spark.read.parquet(args.doc_stats_parquets_path)

            setu.remove_documents(
                df=df,
                doc_stats_df=doc_stats_df,
                docs_per_partition=args.samples_per_partition,
                doc_id_col="doc_id",
                filtered_docs_path=args.filtered_docs_path,
            )

        if not spark_context._jsc.sc().isStopped():
                spark.stop()

    except Exception as e:

        print("Encountered an Error: ", e)
        traceback.print_exc()
        if not spark_context._jsc.sc().isStopped():
            spark.stop()
        raise Exception("Job Failed with above mentioned exception")
        

