#!/bin/usr/env bash

for id in "3" "4" "5";
do
    rm -rf /data/priyam/tmp/*

    lang="hindi"
    parquet_part=$id
    spark_out_part=$id

    gsutil -m cp -r gs://sangraha/parquets/$lang/$parquet_part /data/priyam/sangraha/parquets/$lang

    SETU_DIR=/data/priyam/setu SETU_TMP_DIR=/data/priyam/tmp/ bash setu/scripts/run_setu.sh \
        -i $lang \
        -d "/data/priyam/sangraha/parquets/$lang/$parquet_part/*.parquet" \
        -p 6000 \
        -s /data/priyam/sangraha/spark_out/$lang/$spark_out_part/cleaned_docs \
        -b false \
        -c /data/priyam/tmp \
        -w true \
        -u true \
        -l false \
        -a false \
        -f false \
        -t true \



        -r false \
        -v false \
        -n 128 \
        -o 1 \
        -e "4G" \
        -k "50G" \
        -x 128

    gsutil -m cp -r /data/priyam/sangraha/spark_out/$lang/$spark_out_part/cleaned_docs gs://sangraha/spark_out/$lang/$spark_out_part
    rm -rf /data/priyam/sangraha/parquets/$lang/$parquet_part/*

    # python /data/priyam/setu/setu/helpers/batch_language.py \
    #     -g "/data/priyam/sangraha/parquets/$lang/*/*.parquet" \
    #     -b  0.5 \
    #     -s "/data/priyam/sangraha/spark_out/$lang/dataset/batch_infos/cleaned_docs"

    # SETU_DIR=/data/priyam/setu SETU_TMP_DIR=/data/priyam/tmp/ bash setu/scripts/run_setu.sh \
    #     -i $lang \
    #     -m "/data/priyam/sangraha/spark_out/$lang/$spark_out_part/batch_infos/cleaned_docs/batchs.info" \
    #     -p 6000 \
    #     -s /data/priyam/sangraha/spark_out/$lang/$spark_out_part/cleaned_docs \
    #     -b false \
    #     -c /data/priyam/tmp \
    #     -u true \
    #     -l false \
    #     -a false \
    #     -f false \
    #     -t true \
    #     -r false \
    #     -v false \
    #     -n 16 \
    #     -o 8 \
    #     -e "50G" \
    #     -k "50G" \
    #     -x 128

    python /data/priyam/setu/setu/helpers/batch_language.py \
        -g "/data/priyam/sangraha/spark_out/$lang/$spark_out_part/cleaned_docs/cleaned_docs/*/*.parquet" \
        -b 5 \
        -s "/data/priyam/sangraha/spark_out/$lang/$spark_out_part/batch_infos/lid_segregation"

    SETU_DIR=/data/priyam/setu SETU_TMP_DIR=/data/priyam/tmp/ bash setu/scripts/run_setu.sh \
        -i $lang \
        -m "/data/priyam/sangraha/spark_out/$lang/$spark_out_part/batch_infos/lid_segregation/batchs.info" \
        -p 999 \
        -s /data/priyam/sangraha/spark_out/$lang/$spark_out_part/lid_segregation \
        -b false \
        -c /data/priyam/tmp \
        -w false \
        -u false \
        -l true \
        -a false \
        -f false \
        -t true \
        -r false \
        -v false \
        -n 128 \
        -o 1 \
        -e "4G" \
        -k "50G" \
        -x 128

    gsutil -m cp -r /data/priyam/sangraha/spark_out/$lang/$spark_out_part/lid_segregation gs://sangraha/spark_out/$lang/$spark_out_part
    rm -rf /data/priyam/sangraha/spark_out/$lang/$spark_out_part/cleaned_docs

    # SETU_DIR=/data/priyam/setu SETU_TMP_DIR=/data/priyam/tmp/ bash setu/scripts/run_setu.sh \
    #     -i $lang \
    #     -d "/data/priyam/sangraha/spark_out/$lang/$spark_out_part/lid_segregation/doc_lid/*/*/*.parquet" \
    #     -p 18000 \
    #     -s /data/priyam/sangraha/spark_out/$lang/$spark_out_part/analysis \
    #     -b false \
    #     -c /data/priyam/tmp \
    #     -w false \
    #     -u false \
    #     -l false \
    #     -a true \
    #     -f false \
    #     -t true \
    #     -r false \
    #     -v false \
    #     -n 128 \
    #     -o 1 \
    #     -e "4G" \
    #     -k "50G" \
    #     -x 128

    python /data/priyam/setu/setu/helpers/batch_language.py \
        -g "/data/priyam/sangraha/spark_out/$lang/$spark_out_part/lid_segregation/doc_lid/*/*/*.parquet" \
        -b 10 \
        -s "/data/priyam/sangraha/spark_out/$lang/$spark_out_part/batch_infos/analysis"

    SETU_DIR=/data/priyam/setu SETU_TMP_DIR=/data/priyam/tmp/ bash setu/scripts/run_setu.sh \
        -i $lang \
        -m "/data/priyam/sangraha/spark_out/$lang/$spark_out_part/batch_infos/analysis/batchs.info" \
        -p 18000 \
        -s /data/priyam/sangraha/spark_out/$lang/$spark_out_part/analysis \
        -b false \
        -c /data/priyam/tmp \
        -w false \
        -u false \
        -l false \
        -a true \
        -f false \
        -t true \
        -r false \
        -v false \
        -n 128 \
        -o 1 \
        -e "4G" \
        -k "50G" \
        -x 128

    gsutil -m cp -r /data/priyam/sangraha/spark_out/$lang/$spark_out_part/analysis gs://sangraha/spark_out/$lang/$spark_out_part
    gsutil -m cp -r /data/priyam/sangraha/spark_out/$lang/$spark_out_part/batch_infos gs://sangraha/spark_out/$lang/$spark_out_part
    rm -rf /data/priyam/sangraha/spark_out/$lang/$spark_out_part/lid_segregation
    rm -rf /data/priyam/sangraha/spark_out/$lang/$spark_out_part/analysis
    
done