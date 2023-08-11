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

    args = parser.parse_args()

    return args

if __name__ == "__main__":

    args = parse_args()

    setu = Setu(config_file=args.config)

    # wait for 0.5 sec for Spark job to complete
    # spark_timer = threading.Timer(0.5, timer_elapsed)
    # spark_timer.start()

    spark = SparkSession \
                .builder \
                .appName(setu.config.appname) \
                .getOrCreate()
        
    spark_context = spark.sparkContext
    
    try:
        df = spark.read.parquet(
            args.parquet_glob_path,
        )

        # print(df.schema)

        setu.run_spark_pipeline(
            df,
            cols_to_use=[
                "doc_id", "url", "source", "text", "timestamp", 
                "language", "local_path", "successful_extraction"
            ],
            doc_id_col="doc_id",
            text_col="text",
            docs_per_partition=args.samples_per_partition
        )

    except Exception as e:
        print("Encountered an Error: ", e)
        traceback.print_exc()
    finally:
        if not spark_context._jsc.sc().isStopped():
            spark.stop()

