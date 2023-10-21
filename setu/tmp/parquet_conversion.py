import argparse
import os
from typing import Dict, Iterable, List, Tuple
from pyspark.sql import SparkSession
from pyspark import SparkContext, TaskContext
from pyspark.sql.functions import col
from pyspark.sql.types import (
    BooleanType,
    StringType, 
    StructType, 
    StructField,
    Row
)
from pyspark.sql.functions import (
    udf,
    posexplode,
    col,
    rand
)
import json
import time
from functools import partial
import math
import trafilatura 
import glob

ROW_COUNT = 2000

def read_files_from_list(idx, mapping: Iterable[List[str]], base_dir: str, lang: str):
    task_context: TaskContext = TaskContext.get()  # type: ignore

    # The path used below will be local to the worker's physical machine.
    # Currently, we are using only 1 machine hence, this is fine. For a cluster,
    # with lots of instances, please push the parquet file to a centralized storage
    # like s3 or gcp bucket.
    parquet_save_path = os.path.join(
        base_dir, lang, f"{task_context.partitionId()}.parquet"
    )

    print(f"Starting processing of partition - {task_context.partitionId()}")

    for website_jsons in mapping:
        path = website_jsons["value"]
        print(f"Performing extraction on: {path}")
        try:
            with open(path, "r") as jf:
                content = json.load(jf)
            out["doc_id"].append(path.split("/")[-1])
            out["url"].append(content["url"])
            out["source"].append(content["source"])
            out["timestamp"].append(content["timestamp"])
            out["language"].append(lang)
            out["html"].append(content["html"])

            out = [
                path.split("/")[-1],
                content["url"],
                content["source"],
                content["timestamp"],
                lang,
                content["html"]
            ]
            yield out
        except Exception as e:
            print("Error - ", e)

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

def convert_to_parquet_spark(
    web_json_mapping: List[List[str]],
    count_per_partition: int,
    output_path: str,
    lang: str,
):
    save_website_level_parquets = partial(
        read_files_from_list, base_dir=output_path, lang=lang
    )

    os.makedirs(os.path.join(output_path, lang), exist_ok=True)

    # Create SparkSession
    spark: SparkSession = SparkSession.builder.appName("JSONs->Trafilatura->Parquet").getOrCreate()  # type: ignore

    sc = spark.sparkContext

    n_splits = int(math.ceil(len(web_json_mapping) / count_per_partition))

    print(
        f"Total Website-JSON mappings: {len(web_json_mapping)}, No.of split: {n_splits}"
    )

    jsons_path_df = spark.createDataFrame(web_json_mapping, StringType())

    jsons_path_df = salting(jsons_path_df, n_splits)

    result_schema = StructType([
        StructField("doc_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("source", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("language", StringType(), True),
        StructField("html", StringType(), True),
    ])

    json_rdd = jsons_path_df.rdd.mapPartitionsWithIndex(save_website_level_parquets)

    df = spark.createDataFrame(json_rdd, schema=result_schema)

    df = salting(df, n_splits)

    df.write.mode("overwrite") \
        .parquet(os.path.join(output_path, lang))

    spark.stop()


if __name__ == "__main__":

    json_paths = glob.glob(<add glob path here>)
    lang = <add you lang here>

    print("Starting conversion....")

    base_dir = "/mnt/sangraha/others/parquets"
    os.makedirs(base_dir, exist_ok=True)

    start = time.time()

    convert_to_parquet_spark(json_paths, ROW_COUNT, base_dir, lang=lang)

    end = time.time() - start

    print(f"Converted {lang} to parquet in ...... ", end)



    # mc cp -r sangraha_down/ai4b-public-nlu-nlg/sangraha/crawls_inter-dump/html/marathi/ /mnt/sangraha/web_crawls_new/