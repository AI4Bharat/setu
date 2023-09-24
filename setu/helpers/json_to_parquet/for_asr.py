import argparse
import os
from typing import Dict, Iterable, List, Tuple
from pyspark.sql import SparkSession
from pyspark import SparkContext, TaskContext
import json
import time
import pyarrow as pa
import pyarrow.parquet as pq
from functools import partial
import math
import trafilatura 

ROW_COUNT = 2000


def read_files_from_list(mapping: Iterable[List[str]], base_dir: str, lang: str):
    task_context: TaskContext = TaskContext.get()  # type: ignore

    # The path used below will be local to the worker's physical machine.
    # Currently, we are using only 1 machine hence, this is fine. For a cluster,
    # with lots of instances, please push the parquet file to a centralized storage
    # like s3 or gcp bucket.
    parquet_save_path = os.path.join(
        base_dir, lang, f"{task_context.partitionId()}.parquet"
    )

    # TODO: Will be used for ASR pipeline

    table = pa.table(out)
    with pq.ParquetWriter(
        parquet_save_path, table.schema, compression="SNAPPY"
    ) as pq_writer:
        pq_writer.write_table(table)

    print(
        f"Completed {task_context.partitionId()} - It contained {len(out['doc_id'])} jsons"
    )


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
    spark: SparkSession = SparkSession.builder.appName("JSONs->Parquet").getOrCreate()  # type: ignore

    sc = spark.sparkContext

    n_splits = int(math.ceil(len(web_json_mapping) / ROW_COUNT))

    print(
        f"Total Website-JSON mappings: {len(web_json_mapping)}, No.of split: {n_splits}"
    )

    mapping_rdd = sc.parallelize(web_json_mapping, n_splits)

    mapping_rdd.foreachPartition(save_website_level_parquets)

    spark.stop()


if __name__ == "__main__":
    with open("/mnt/phallm-data/datasets/malayalam1.json", "r") as json_f:
        mal_out: List[List[str]] = json.load(json_f)

    print("Starting conversion....")

    base_dir = "/mnt/phallm-data/datasets/sangraha/parquets-trafilatura"
    os.makedirs(base_dir, exist_ok=True)

    start = time.time()

    convert_to_parquet_spark(mal_out, 1, base_dir, lang="malayalam")

    end = time.time() - start

    print("Converted malayalam to parquet in ...... ", end)
