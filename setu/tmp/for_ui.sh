#!/bin/bash

for lang in "Assamese" "Bengali" "Gujarati" "Hindi" "Kannada" "Malayalam" "Marathi" "Nepali" "Oriya" "Punjabi" "Sanskrit" "Tamil" "Telugu" "Urdu";
do
    SAVE_LANG=`echo "$lang" | tr '[:upper:]' '[:lower:]'`
    gsutil -m cp -r gs://sangraha/pdfs_out_full/assamese/1_2_3_4_5/cleaned_docs/cleaned_docs/dataset gs://
done

