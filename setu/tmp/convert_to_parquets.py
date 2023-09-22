import pyspark
from pyspark.sql import SparkSession
import argparse

def parse_args():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--parquet_glob",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--spark_save_parquets",
        type=str,
        required=True,
    )

    args = parser.parse_args()

    return args

def main():

    args = parse_args()

    spark = SparkSession \
                .builder \
                .appName("Parquet conversion using spark") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.sql.broadcastTimeout", "36000") \
                .config("spark.driver.maxResultSize", "0") \
                .config('spark.sql.autoBroadcastJoinThreshold', '-1') \
                .config('spark.sql.adaptive.enabled', 'true') \
                .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
                .config('spark.speculation', 'true') \
                .getOrCreate()

    spark_context = spark.sparkContext

    df = spark.read.format("parquet").options(header='true', inferschema='true').load(args.parquet_glob)

    df.write.mode("overwrite").parquet(args.spark_save_parquets)


if __name__ == "__main__":

    main()
