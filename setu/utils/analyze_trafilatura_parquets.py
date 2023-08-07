import argparse
import os
from typing import Dict, Iterable, List, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark import SparkContext, TaskContext
import json
import time
import pyarrow as pa
import pyarrow.parquet as pq
from functools import partial
import math
import trafilatura

def analyze_parquets(df):
    df = df.filter(df.successful_extraction == False)
    df.select("url").show()
    print(f"Total Document with failed extraction: {df.count()}")
    return df

if __name__ == "__main__":

    PATH = "~/../../../datasets/sangraha/parquets-trafilatura/*/*"

    spark: SparkSession = SparkSession \
                            .builder \
                            .master("local") \
                            .appName("Data Pipeline") \
                            .getOrCreate()

    sc = spark.sparkContext

    df = spark.read.parquet(PATH)

    df_len = df.count()

    n_splits = math.ceil(df_len/200000)

    df = df.repartition(n_splits)
    
    print(f"Repartitioned the data into - {n_splits} partitions")

    print("Starting analysis....")

    start = time.time()

    _ = analyze_parquets(df)

    end = time.time() - start

    print("Converted malayalam to parquet in ...... ", end)

    spark.stop()