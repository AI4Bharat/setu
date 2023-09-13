#!/bin/usr/env bash

lang="hindi"
spark_out_part="dataset"

# mkdir -p /data/priyam/sangraha/spark_out/$lang/$spark_out_part
mkdir -p /data/priyam/sangraha/dedup/minhash/$lang


cd /data/priyam/setu/text-dedup

python -m text_dedup.minhash \
    --path "parquet" \
    --name "sangraha-$lang" \
    --split "train" \
    --data_files "/data/priyam/sangraha/spark_out/$lang/*/filtered_docs/filtered_docs/*/*.parquet" \
    --cache_dir "/data/priyam/cache" \
    --output "/data/priyam/sangraha/dedup/minhash/$lang" \
    --column "text" \
    --batch_size 10000

# gsutil -m cp -r /data/priyam/sangraha/dedup/minhash/$lang  gs://sangraha/dedup/minhash

# rm -rf /data/priyam/sangraha/spark_out/$lang
# rm -rf /data/priyam/sangraha/dedup/minhash/$lang
# rm -rf /data/priyam/cache/*









# export CLUSTER_NAME=setu-minhash-dedup-small
# export PROJECT_ID="sangraha-396106"
# export REGION=asia-south1
# # export ZONE=asia-south1-a

# export INPUT_GCS_PATH="gs://sangraha/spark_out/$lang/$spark_out_part/filtered_docs/filtered_docs/*/*.parquet"
# export OUTPUT_GCS_PATH="gs://sangraha/dedup/minhash/$lang"


# gcloud dataproc clusters create $CLUSTER_NAME \
#     --enable-component-gateway \
#     --region $REGION \
#     --zone $ZONE \
#     --master-machine-type c2d-standard-16 \
#     --master-boot-disk-size 500 \
#     --num-workers 3 \
#     --worker-machine-type c2d-standard-16 \
#     --worker-boot-disk-size 500 \
#     --image-version 2.0-debian10 \
#     --project $PROJECT_ID

# gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME}\
#     --region $REGION \
#     --jars gs://spark-lib/bigquery/spark-3.3-bigquery-0.32.2.jar \
#     --driver-log-levels root=FATAL,__main__=DEBUG \
#     --properties="spark.executor.memory"="50g","spark.driver.memory"="8g","spark.executor.cores"="14" \
#     /data/priyam/setu/text-dedup/text_dedup/minhash_spark.py \
#     -- \
#     --column "text" \
#     --input $INPUT_GCS_PATH \
#     --output $OUTPUT_GCS_PATH