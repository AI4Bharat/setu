#!/bin/bash

for lang in "assamese" "bodo" "dogri" "kashmiri" "konkani" "maithili" "manipuri" "sanskrit" "santhali"; do

    gcloud dataproc workflow-templates instantiate minhash_low_template \
        --region="asia-south1" \
        --parameters="CLUSTER_NAME=$lang-minhash-small,MAIN_PYTHON_FILE=gs://sangraha/setu/minhash_spark.py,COLUMN=text,THRESHOLD=0.7,NGRAM_SIZE=5,MIN_LENGTH=5,NUM_PERM=256,INPUT=gs://sangraha/spark_out_dataproc/$lang/dataset/filtered_docs/fitlered_docs/*/*.parquet,OUTPUT=gs://sangraha/minhash_dataproc/$lang"

done

for lang in "gujarati" "kannada" "oriya" "punjabi" "sindhi"; do

    gcloud dataproc workflow-templates instantiate minhash_medium_template \
        --region="asia-south1" \
        --parameters="CLUSTER_NAME=$lang-minhash-medium,MAIN_PYTHON_FILE=gs://sangraha/setu/minhash_spark.py,COLUMN=text,THRESHOLD=0.7,NGRAM_SIZE=5,MIN_LENGTH=5,NUM_PERM=256,INPUT=gs://sangraha/spark_out_dataproc/$lang/dataset/filtered_docs/fitlered_docs/*/*.parquet,OUTPUT=gs://sangraha/minhash_dataproc/$lang"

done

for lang in "bengali" "english" "hindi" "malayalam" "marathi" "nepali" "tamil" "telugu" "urdu"; do

    gcloud dataproc workflow-templates instantiate minhash_large_template \
        --region="asia-south1" \
        --parameters="CLUSTER_NAME=$lang-minhash-large,MAIN_PYTHON_FILE=gs://sangraha/setu/minhash_spark.py,COLUMN=text,THRESHOLD=0.7,NGRAM_SIZE=5,MIN_LENGTH=5,NUM_PERM=256,INPUT=gs://sangraha/spark_out_dataproc/$lang/dataset/filtered_docs/fitlered_docs/*/*.parquet,OUTPUT=gs://sangraha/minhash_dataproc/$lang"

done