#!/bin/usr/env bash

mkdir -p /data/priyam/sangraha/dedup/minhash

# for lang in "assamese" "bengali" "bodo" "dogri" "english" "gujarati" "hindi" "kannada" "kashmiri" "konkani" "maithili" "malayalam" "manipuri" "marathi" "nepali" "oriya" "punjabi" "sanskrit" "sindhi" "tamil" "telugu" "urdu"; 
# do
#     gsutil -m cp -r gs://sangraha/dedup/minhash/parquets/$lang /data/priyam/sangraha/dedup/minhash/parquets
# done

gsutil -m cp -r gs://sangraha/dedup/minhash/parquets /data/priyam/sangraha/dedup/minhash
