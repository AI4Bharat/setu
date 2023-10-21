#!/bin/usr/env bash

for lang in "bengali" "gujarati" "hindi" "kannada" "malayalam" "marathi" "nepali" "oriya" "punjabi" "sanskrit" "tamil" "telugu" "urdu";
do
    gsutil -m mv -r gs://sangraha/pdfs_out/$lang/1/lid/lid/* gs://sangraha/pdfs_out/$lang/1/lid_segregation/doc_lid/dataset
    gsutil -m rm -r gs://sangraha/pdfs_out/$lang/1/lid

    gsutil -m mv -r gs://sangraha/pdfs_out/$lang/1/doc_clean/cleaned_doc/* gs://sangraha/pdfs_out/$lang/1/cleaned_docs/cleaned_docs/dataset
    gsutil -m mv -r gs://sangraha/pdfs_out/$lang/1/doc_clean/symbol_heavy/* gs://sangraha/pdfs_out/$lang/1/cleaned_docs/symbol_heavy/dataset
    gsutil -m rm -r gs://sangraha/pdfs_out/$lang/1/doc_clean

    gsutil -m mv -r gs://sangraha/pdfs_out/$lang/1/fnf gs://sangraha/pdfs_out/$lang/1/filtering
    gsutil -m rm -r gs://sangraha/pdfs_out/$lang/1/fnf

    gsutil -m mv -r gs://sangraha/pdfs_out/$lang/1/document_removal gs://sangraha/pdfs_out/$lang/1/filtered_docs
    gsutil -m rm -r gs://sangraha/pdfs_out/$lang/1/document_removal
done
