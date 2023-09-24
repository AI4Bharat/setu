#!/bin/usr/env bash


for lang in "assamese" "bengali" "bodo" "dogri" "english" "gujarati" "hindi" "kannada" "kashmiri" "konkani" "maithili" "malayalam" "manipuri" "marathi" "nepali" "oriya" "punjabi" "sanskrit" "sindhi" "tamil" "telugu" "urdu"; 
do
    mkdir -p /data/priyam/sangraha/dedup/minhash/$lang
    gsutil -m cp -r gs://sangraha/minhash_dataproc/$lang/__pid__=* /data/priyam/sangraha/dedup/minhash/$lang
done

# gsutil -m cp -r gs://sangraha/dedup/minhash/parquets /data/priyam/sangraha/dedup/minhash
