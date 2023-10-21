#!/bin/usr/env bash

# for LANG in "assamese" "bodo" "dogri" "gujarati" "kannada" "kashmiri" "konkani" "maithili" "manipuri" "nepali" "oriya" "punjabi" "santhali" "sindhi" "bengali" "english" "hindi" "malayalam" "marathi" "sanskrit" "tamil" "telugu" "urdu";
# do
#     mkdir -p /data/priyam/sangraha/viz_dc/$LANG
#     gsutil -m cp -r gs://sangraha/spark_out_test/$LANG/visualization /data/priyam/sangraha/viz_dc/$LANG
# done

# for LANG in "gujarati";
# do
#     mkdir -p /data/priyam/sangraha/viz_new/$LANG
#     gsutil -m cp -r gs://sangraha/spark_out/$LANG/visualization /data/priyam/sangraha/viz_new/$LANG
# done

for LANG in "assamese" "bengali" "gujarati" "hindi" "kannada" "malayalam" "marathi" "nepali" "oriya" "punjabi" "sanskrit" "tamil" "telugu" "urdu";
do
    mkdir -p /data/priyam/sangraha/pdfs_out/$LANG/visualization
    gsutil -m cp -r gs://sangraha/pdfs_out/$LANG/visualization/2 /data/priyam/sangraha/pdfs_out/$LANG/visualization
done
