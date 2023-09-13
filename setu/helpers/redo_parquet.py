import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, spark_partition_id
from math import ceil

def parse_args():

    parser = argparse.ArgumentParser(description="")

    parser.add_argument(
        "-g",
        "--glob_path",
        type=str,
        help="Glob Path to send to PySpark Parquet reader",
    )

    parser.add_argument(
        "-s",
        "--save_folder",
        type=str,
        help="Folder to save the new parquets",
    )

    parser.add_argument(
        "-p",
        "--docs_per_partition",
        type=int,
        help="Records per partition",
    )
    
    args = parser.parse_args()

    return args

def salting(df, n_splits):
    # Number of salt buckets
    num_buckets = n_splits  # Adjust based on your data size and level of skewness

    # Adding a salt column to the DataFrame
    df = df.withColumn("salt", (rand() * num_buckets).cast("int"))

    # df.show(1000)

    # Repartition based on the salted key to ensure more even distribution
    df = df.repartition(n_splits, "salt")

    df = df.drop("salt")

    return df

def set_split_count_and_salt(df, docs_per_partition):

    df_total_rows = df.count()

    n_splits = ceil(df_total_rows/docs_per_partition)

    print(f"When required data will be repartitioned into - {n_splits} partitions")

    df = salting(df, n_splits)

    return df

if __name__ == "__main__":

    args = parse_args()

    spark = SparkSession \
                .builder \
                .appName("Rewrite parquets records") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.sql.broadcastTimeout", "36000") \
                .config("spark.driver.maxResultSize", "0") \
                .config('spark.sql.autoBroadcastJoinThreshold', '-1') \
                .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
                .getOrCreate()

    spark_context = spark.sparkContext

    parquet_df = spark.read.format("parquet").options(header='true', inferschema='true').load(args.glob_path)

    parquet_df = set_split_count_and_salt(parquet_df, args.docs_per_partition)

    parquet_df = parquet_df.select(*[
        "doc_id", "url", "source", "text", 
        "language", "successful_extraction"
    ])

    parquet_df.withColumn("partition_id", spark_partition_id()).groupBy("partition_id").count().sort("partition_id").show(70000)


    parquet_df.write.mode("overwrite") \
            .parquet(args.save_folder)


"""
 0   doc_id                 1024 non-null   object
 1   url                    1024 non-null   object
 2   source                 1024 non-null   object
 3   timestamp              1024 non-null   object
 4   language               1024 non-null   object
 5   local_path             1024 non-null   object
 6   successful_extraction  1024 non-null   bool  
 7   title                  1024 non-null   object
 8   description            0 non-null      object
 9   text                   1024 non-null   object
 10  comments               1024 non-null   object
 11  author                 1024 non-null   object
 12  hostname               1024 non-null   object
 13  sitename               1024 non-null   object
 14  date                   1024 non-null   object
 15  categories             1024 non-null   object
 16  tags                   1024 non-null   object
 17  fingerprint            0 non-null      object
 18  id                     0 non-null      object
 19  license                0 non-null      object
 20  body                   0 non-null      object
 21  commentsbody           0 non-null      object
 22  raw_text               0 non-null      object
 23  image                  0 non-null      object
 24  pagetype
"""