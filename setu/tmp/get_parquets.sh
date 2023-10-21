#!/bin/usr/env bash

for lang in "bengali" "gujarati" "hindi" "sanskrit" "tamil" "telugu" "urdu";
do

    mkdir -p /data/priyam/sangraha/pdfs_out/$lang/2
    gsutil -m cp -r gs://sangraha/pdfs_out/$lang/2/json2parquets /data/priyam/sangraha/pdfs_out/$lang/2

done