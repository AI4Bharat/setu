#!/bin/usr/env bash


# for lang in "assamese" "bengali" "bodo" "dogri" "english" "gujarati" "hindi" "kannada" "kashmiri" "konkani" "maithili" "malayalam" "manipuri" "marathi" "nepali" "oriya" "punjabi" "sanskrit" "sindhi" "tamil" "telugu" "urdu"; 
# for lang in "hindi";
for lang in  "assamese" "bengali" "gujarati" "hindi" "kannada" "malayalam" "marathi" "nepali" "oriya" "punjabi" "sanskrit" "tamil" "telugu" "urdu";
do
    mkdir -p /data/priyam/sangraha/pdfs_out_dedup/minhash/$lang
    gsutil -m cp -r gs://sangraha/pdfs_out_dedup/minhash/$lang/__pid__* /data/priyam/sangraha/pdfs_out_dedup/minhash/$lang
    # mkdir -p /data/priyam/sangraha/dedup/minhash_new/$lang
    # gsutil -m cp -r gs://sangraha/dedup/minhash_new/hindi/__pid__* /data/priyam/sangraha/dedup/minhash_new/$lang
done

# gsutil -m cp -r gs://sangraha/dedup/minhash/parquets /data/priyam/sangraha/dedup/minhash
