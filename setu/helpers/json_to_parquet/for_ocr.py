import argparse
import os
from pyspark.sql import SparkSession
from pyspark import SparkContext, TaskContext
import json
import time
import pyarrow as pa
import pyarrow.parquet as pq
from functools import partial
import math
import glob

def parse_args():

    parser = argparse.ArgumentParser(description="")

    parser.add_argument(
        "--json_glob",
        type=str,
        required=True,
        help="Glob expression for JSONs"
    )

    parser.add_argument(
        "--rows_count",
        type=int,
        default=2000,
        required=False,
        help="Approx No.of samples to place per partition",
    )

    parser.add_argument(
        "--base_dir",
        type=str,
        required=True,
        help="Base Directory where to store in parquets",
    )

    parser.add_argument(
        "--lang",
        type=str,
        required=True,
        help="Language"
    )

    args = parser.parse_args()

    return args

def read_files_from_list(json_list, base_dir, lang):
    task_context = TaskContext.get()  # type: ignore

    # The path used below will be local to the worker's physical machine.
    # Currently, we are using only 1 machine hence, this is fine. For a cluster,
    # with lots of instances, please push the parquet file to a centralized storage
    # like s3 or gcp bucket.
    parquet_save_path = os.path.join(
        base_dir, f"{task_context.partitionId()}.parquet"
    )

    out = {
        "doc_id": [],
        "url": [],
        "source": [],
        "language": [],
        "text": [],
    }

    print(f"Starting processing of partition - {task_context.partitionId()}")

    for j in json_list:
        print(f"Performing extraction on: {j}")
        with open(j, "r") as jf:
            content = json.load(jf)
        out["doc_id"].append(content["doc_id"])
        out["url"].append(content["url"])
        out["source"].append(content["source"])
        out["language"].append(lang)
        out["text"].append(content["text"])

    table = pa.table(out)
    with pq.ParquetWriter(
        parquet_save_path, table.schema, compression="SNAPPY"
    ) as pq_writer:
        pq_writer.write_table(table)

    print(
        f"Completed {task_context.partitionId()} - It contained {len(out['doc_id'])} jsons"
    )


def convert_to_parquet_spark(
    json_list,
    count_per_partition,
    output_path,
    lang,
):
    save_website_level_parquets = partial(
        read_files_from_list, base_dir=output_path, lang=lang
    )

    # Create SparkSession
    spark: SparkSession = SparkSession.builder.appName("JSONs->Parquet").getOrCreate()  # type: ignore

    sc = spark.sparkContext

    n_splits = int(math.ceil(len(json_list) / ROW_COUNT))

    print(
        f"Total Website-JSON mappings: {len(json_list)}, No.of split: {n_splits}"
    )

    mapping_rdd = sc.parallelize(json_list, n_splits)

    mapping_rdd.foreachPartition(save_website_level_parquets)

    spark.stop()


if __name__ == "__main__":

    args = parse_args()

    json_list = glob.glob(args.json_glob)

    print("Starting conversion....")

    os.makedirs(args.base_dir, exist_ok=True)

    start = time.time()

    convert_to_parquet_spark(json_list, args.rows_count, args.base_dir, lang=args.lang)

    end = time.time() - start

    print(f"Converted {args.lang} for glob path {args.json_glob} to parquet in ...... ", end)
