#!/bin/usr/env bash

# for lang in "assamese" "bengali" "bodo" "dogri" "english" "gujarati" "kashmiri" "konkani" "maithili" "malayalam" "manipuri" "oriya" "punjabi" "santhali" "sindhi" "tamil" "telugu" "urdu";
for lang in "hindi";
do
    gsutil -m cp -r spark_out/$lang gs://sangraha/spark_out    
done