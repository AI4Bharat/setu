#!/bin/usr/env bash

for lang in "tamil" "telugu" "urdu";
do
    # gsutil -m cp -r /data/priyam/sangraha/dedup/exact/parquets/$lang gs://sangraha/dedup/exact/parquets

    rm -rf /data/priyam/sangraha/dedup/exact/$lang
    # rm -rf /data/priyam/sangraha/dedup/exact/parquets/$lang
done