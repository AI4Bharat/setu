#!/bin/usr/env bash

for lang in "Assamese" "Bengali" "Gujarati" "Kannada" "Malayalam" "Marathi" "Nepali" "Oriya" "Punjabi" "Telugu";
do
    save_lang=`echo "$lang" | tr '[:upper:]' '[:lower:]'`
    spark-submit \
        --master spark://SPK-DGX-O2:7077 \
        --num-executors 128 \
        --executor-cores 1 \
        --executor-memory "5G" \
        --driver-memory "10G" \
        /data/priyam/setu/setu/helpers/json_to_parquet/for_ocr.py \
        --json_glob "/data/priyam/sangraha/pdf_texts/1/$lang/*.json" \
        --rows_count 2000 \
        --base_dir "/data/priyam/sangraha/pdf_parquets/$save_lang/1" \
        --lang "$save_lang"
done