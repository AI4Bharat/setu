#!/bin/usr/env bash

rm -rf /data/priyam/tmp/*

lang="telugu"
spark_out_part="dataset"

mkdir -p /data/priyam/sangraha/spark_out/$lang/$spark_out_part

gsutil -m cp -r gs://sangraha/spark_out/$lang/$spark_out_part/analysis /data/priyam/sangraha/spark_out/$lang/$spark_out_part

SETU_DIR=/data/priyam/setu SETU_TMP_DIR=/data/priyam/tmp/ bash setu/scripts/run_setu.sh \
    -i $lang \
    -d "/data/priyam/sangraha/spark_out/$lang/$spark_out_part/analysis/doc_stats/*/$lang/*.parquet" \
    -p 18000 \
    -s /data/priyam/sangraha/spark_out/$lang/$spark_out_part/filtering \
    -b false \
    -c /data/priyam/tmp \
    -w false \
    -u false \
    -l false \
    -a false \
    -f true \
    -t true \
    -r false \
    -v false \
    -n 16 \
    -o 8 \
    -e "32G" \
    -k "50G" \
    -x 128

SETU_DIR=/data/priyam/setu SETU_TMP_DIR=/data/priyam/tmp/ bash setu/scripts/run_setu.sh \
    -i $lang \
    -d "/data/priyam/sangraha/spark_out/$lang/$spark_out_part/analysis/analysis/*/$lang/*.parquet" \
    -p 18000 \
    -s /data/priyam/sangraha/spark_out/$lang/$spark_out_part/filtered_docs \
    -b false \
    -c /data/priyam/tmp \
    -w false \
    -u false \
    -l false \
    -a false \
    -f false \
    -t true \
    -r true \
    -q "/data/priyam/sangraha/spark_out/$lang/$spark_out_part/filtering/filtered_doc_stats/*/*.parquet" \
    -v false \
    -n 16 \
    -o 8 \
    -e "32G" \
    -k "50G" \
    -x 128

gsutil -m cp -r /data/priyam/sangraha/spark_out/$lang/$spark_out_part/filtering  gs://sangraha/spark_out/$lang/$spark_out_part
gsutil -m cp -r /data/priyam/sangraha/spark_out/$lang/$spark_out_part/filtered_docs  gs://sangraha/spark_out/$lang/$spark_out_part