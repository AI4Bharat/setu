#!/bin/usr/env bash

# for lang in "hindi";
# for lang in  "assamese" "bengali" "gujarati" "hindi" "kannada" "malayalam" "marathi" "nepali" "oriya" "punjabi" "sanskrit" "tamil" "telugu" "urdu";
for lang in "assamese" "bengali" "bodo" "dogri" "english" "gujarati" "hindi" "kannada" "kashmiri" "konkani" "maithili" "malayalam" "manipuri" "marathi" "nepali" "oriya" "punjabi" "sanskrit" "sindhi" "tamil" "telugu" "urdu"; 
do
    # mkdir -p /data/priyam/sangraha/pdfs_out_dedup/minhash/$lang
    # gsutil -m cp -r gs://sangraha/pdfs_out_dedup/minhash/$lang/cluster /data/priyam/sangraha/pdfs_out_dedup/minhash/$lang
    mkdir -p /data/priyam/sangraha/dedup/minhash/$lang
    gsutil -m cp -r gs://sangraha/dedup/minhash/$lang/cluster /data/priyam/sangraha/dedup/minhash/$lang
done

