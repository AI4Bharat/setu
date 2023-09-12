#!/bin/usr/env bash

lang="dogri"
spark_out_part="dataset"

export CLUSTER_NAME=setu-minhash-dedup-small
export PROJECT_ID="sangraha-396106"
export REGION=asia-south1
export ZONE=asia-south1-a

export INPUT_GCS_PATH="gs://sangraha/spark_out/$lang/$spark_out_part/filtered_docs/filtered_docs/*/*.parquet"
export OUTPUT_GCS_PATH="gs://sangraha/dedup/minhash/$lang"


# gcloud dataproc clusters create $CLUSTER_NAME \
#     --enable-component-gateway \
#     --region $REGION \
#     --zone $ZONE \
#     --master-machine-type c2d-standard-16 \
#     --master-boot-disk-size 500 \
#     --num-workers 3 \
#     --worker-machine-type c2d-standard-16 \
#     --worker-boot-disk-size 500 \
#     --image-version "2.1-debian11" \
#     --project $PROJECT_ID

gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME}\
    --region $REGION \
    --jars gs://spark-lib/bigquery/spark-3.3-bigquery-0.32.2.jar \
    --driver-log-levels root=FATAL,__main__=DEBUG \
    --properties="spark.executor.memory"="50g","spark.driver.memory"="8g","spark.executor.cores"="14" \
    /data/priyam/setu/text-dedup/text_dedup/minhash_spark.py \
    -- \
    --column "text" \
    --threshold "0.7" \
    --ngram_size "5" \
    --min_length "5" \
    --num_perm "256" \
    --input $INPUT_GCS_PATH \
    --output $OUTPUT_GCS_PATH \
    --debug

# PYTHON_SCRIPT="/data/priyam/setu/text-dedup/text_dedup/minhash_spark.py"

# spark-submit \
#     --master "spark://SPK-DGX-O1:7077" \
#     --driver-java-options "-Djava.io.tmpdir=$SETU_TMP_DIR" \
#     --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=$SETU_TMP_DIR" \
#     --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=$SETU_TMP_DIR" \
#     --conf "spark.worker.dir=$SETU_TMP_DIR" \
#     --conf "spark.local.dir=$SETU_TMP_DIR" \
#     --num-executors "6" \
#     --executor-cores "16" \
#     --executor-memory "50g" \
#     --driver-memory "8g" \
#     "$PYTHON_SCRIPT" \
#     --input "/data/priyam/sangraha/spark_out/assamese/*/filtered_docs/filtered_docs/*/*.parquet" \
#     --threshold "0.7" \
#     --ngram_size "5" \
#     --min_length "5" \
#     --num_perm "256" \
#     --column "text" \
#     --output "/data/priyam/sangraha/dedup/minhash/assamese" \
#     --debug


