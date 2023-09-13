#!/bin/usr/env bash

for lang in "bengali" "english" "marathi" "nepali" "oriya" "punjabi" "sanskrit" "santhali" "sindhi" "tamil" "telugu" "urdu";
do
    echo "Running Exact Dedup for $lang" >> "exact_dedup_logs.txt"
    # spark_out_part="dataset"

    mkdir -p /data/priyam/sangraha/dedup/exact/$lang

    gsutil -m cp -r gs://sangraha/dedup/minhash/$lang /data/priyam/sangraha/dedup/minhash

    echo "Downloaded MinHash Dedup for $lang from GCP" >> "exact_dedup_logs.txt"

    cd /data/priyam/setu/text-dedup

    python -m text_dedup.suffix_array \
        --path "arrow" \
        --name "sangraha-$lang" \
        --split "train" \
        --data_files "/data/priyam/sangraha/dedup/minhash/$lang/*.arrow" \
        --cache_dir "/data/priyam/cache" \
        --output "/data/priyam/sangraha/dedup/exact/$lang" \
        --column "text" \
        --google_repo_path "/data/priyam/setu/text-dedup/deduplicate-text-datasets"


    gsutil -m cp -r /data/priyam/sangraha/dedup/exact/$lang  gs://sangraha/dedup/exact

    echo "Uploaded Exact Dedup for $lang to GCP" >> "exact_dedup_logs.txt"

    rm -rf /data/priyam/sangraha/dedup/minhash/$lang
    rm -rf /data/priyam/sangraha/dedup/exact/$lang
    rm -rf /data/priyam/cache/*

    echo "Completed Exact Dedup for $lang" >> "exact_dedup_logs.txt"

done