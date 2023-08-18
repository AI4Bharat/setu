import argparse
from setu import Setu
from pyspark.sql import SparkSession
import threading
import traceback

def parse_args():

    parser = argparse.ArgumentParser(description="Runs Setu")

    parser.add_argument(
        "-c",
        "--config",
        type=str,
        required=True,
        help="Path to config file",
    )

    parser.add_argument(
        "-p",
        "--parquet_glob_path",
        type=str,
        required=True,
        help="Path to folder containing parquets",
    )

    parser.add_argument(
        "-s",
        "--samples_per_partition",
        type=int,
        default=20000,
        required=False,
        help="No.of samples per partition",
    )
    
    parser.add_argument(
        "--save_doc_lid_output",
        action="store_true",
        help="Whether to store lid checkpoint",
    )

    parser.add_argument(
        "-i",
        "--doc_lid_output_path",
        type=str,
        required=False,
        default=None,
        help="Path of the folder store lid checkpoint",
    )

    parser.add_argument(
        "--save_line_stats_output",
        action="store_true",
        help="Whether to store line stats checkpoint",
    )

    parser.add_argument(
        "-l",
        "--line_stats_output_path",
        type=str,
        required=False,
        default=None,
        help="Path of the folder store line stats checkpoint",
    )

    parser.add_argument(
        "--save_doc_stats_output",
        action="store_true",
        help="Whether to store doc stats checkpoint",
    )

    parser.add_argument(
        "-d",
        "--doc_stats_output_path",
        type=str,
        required=False,
        default=None,
        help="Path of the folder store doc stats checkpoint",
    )

    parser.add_argument(
        "--save_nsfw_data",
        action="store_true",
        help="Whether to store nsfw data",
    )

    parser.add_argument(
        "-n",
        "--nsfw_output_path",
        type=str,
        required=False,
        default=None,
        help="Path of the folder to store nsfw data",
    )

    parser.add_argument(
        "-f",
        "--final_output_path",
        type=str,
        required=False,
        default=None,
        help="Path to the folder to store final output",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Whether to add `show()` at each stage.",
    )

    args = parser.parse_args()

    return args

if __name__ == "__main__":

    args = parse_args()

    print("Entered Commandline Arguments: ", args)

    setu = Setu(config_file=args.config)

    spark = SparkSession \
                .builder \
                .appName(setu.config.appname) \
                .getOrCreate()
        
    spark_context = spark.sparkContext
    
    try:
        df = spark.read.parquet(
            args.parquet_glob_path,
        )

        setu.run_spark_pipeline(
            df,
            cols_to_use=[
                "doc_id", "url", "source", "text", "timestamp", 
                "language", "local_path", "successful_extraction"
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
            save_nsfw_data=args.save_nsfw_data,
            nsfw_output_path=args.nsfw_output_path,
            final_output_path=args.final_output_path,
            verbose=args.verbose,
        )

    except Exception as e:
        print("Encountered an Error: ", e)
        traceback.print_exc()
    finally:
        if not spark_context._jsc.sc().isStopped():
            spark.stop()

