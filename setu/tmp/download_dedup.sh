#!/bin/usr/env bash

for lang in "tamil" "telugu" "urdu"; 
do

    gsutil -m cp -r gs://sangraha/dedup/minhash/$lang /data/priyam/sangraha/dedup/minhash

    # gsutil -m cp -r gs://sangraha/dedup/exact/parquets/$lang /data/priyam/sangraha/dedup/exact/parquets

done