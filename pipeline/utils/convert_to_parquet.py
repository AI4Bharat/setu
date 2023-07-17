import argparse
import os
from pyspark.sql import SparkSession
from pyspark import TaskContext
import json
import time
import pyarrow as pa
import pyarrow.parquet as pq
from functools import partial

def read_files_from_list(mapping, base_dir, lang):

    task_context = TaskContext.get()
    
    for website_jsons in mapping:

        website, json_list = website_jsons

        # The path used below will be local to the worker's physical machine.
        # Currently, we are using only 1 machine hence, this is fine. For a cluster, 
        # with lots of instances, please push the parquet file to a centralized storage
        # like s3 or gcp bucket.
        parquet_save_path = os.path.join(base_dir, lang, f"{website}.parquet")

        print(f"Starting processing of partition - {task_context.partitionId()} / {website}....") 
        out = {
            "html": [], "source": [], "url": [], "timestamp": [],
        }
        
        for j in json_list:
            with open(j, "r") as jf:
                content = json.load(jf)
                out["html"] += [content["html"]]
                out["source"] += [content["source"]]
                out["url"] += [content["url"]]
                out["timestamp"] += [content["timestamp"]]

        table = pa.table(out)
        with pq.ParquetWriter(parquet_save_path, table.schema, compression="SNAPPY") as pq_writer:
            pq_writer.write_table(table)

        print(f"Completed {task_context.partitionId()} / {website} - It contained {len(json_list)} jsons") 

def convert_to_parquet_spark(web_json_mapping, count_per_partition, output_path, lang):
    
    save_website_level_parquets = partial(read_files_from_list, base_dir=output_path, lang=lang)

    os.makedirs(os.path.join(output_path, lang), exist_ok=True)
    
    # Create SparkSession
    spark = SparkSession \
            .builder \
            .appName("Convert JSONs to Parquet") \
            .getOrCreate()
    
    sc = spark.sparkContext

    n_splits = int(len(web_json_mapping)/count_per_partition)

    print(f"Total Website-JSON mappings: {len(web_json_mapping)}, No.of split: {n_splits}")

    mapping_rdd = sc.parallelize(web_json_mapping, n_splits)

    mapping_rdd.foreachPartition(save_website_level_parquets)

    spark.stop()

if __name__ == "__main__":

    with open("/mnt/phallm-data/datasets/malayalam_output.json", "r") as json_f:
        mal_out = json.load(json_f)

    print("Starting conversion....")

    base_dir = "/mnt/phallm-data/datasets/sangraha/parquets"
    os.makedirs(base_dir, exist_ok=True)

    start = time.time()

    convert_to_parquet_spark(mal_out.items(), 1, base_dir, lang="malayalam")

    end = time.time() - start

    print("Converted malayalam to parquet in ...... ", end)
