#!/bin/bash

# for lang in "assamese" "bodo" "dogri" "kashmiri" "konkani" "maithili" "manipuri" "sanskrit" "santhali" "gujarati" "kannada" "oriya" "punjabi" "sindhi" "bengali" "english" "hindi" "malayalam" "marathi" "nepali" "tamil" "telugu" "urdu"; do
# "Assamese" 
# for LANG in "Assamese";
# "Sanskrit" "Urdu"
for LANG in "Bengali" "Gujarati" "Hindi" "Kannada" "Malayalam" "Marathi" "Nepali" "Oriya" "Punjabi"  "Tamil" "Telugu";
do
    lang=`echo "$LANG" | tr '[:upper:]' '[:lower:]'`
    # gcloud dataproc workflow-templates instantiate minhash_cluster_selector_template \
    #     --region="asia-south1" \
    #     --parameters="CLUSTER_NAME=minhash-large,MAIN_PYTHON_FILE=gs://sangraha/setu/text-dedup/minhash_spark.py,COLUMN=text,THRESHOLD=0.7,NGRAM_SIZE=5,MIN_LENGTH=5,NUM_PERM=256,INPUT=gs://sangraha/pdfs_out/$lang/1/document_removal/filtered_docs/dataset/*.parquet,OUTPUT=gs://sangraha/pdfs_out_dedup/minhash/$lang"

    export CLUSTER_NAME=minhash-30
    export PROJECT_ID="sangraha-396106"
    export REGION=asia-south1
    export ZONE=asia-south1-a

    export INPUT_GCS_PATH="gs://sangraha/pdfs_out_full/$lang/*/filtered_page2pdfs/page2pdfs/dataset/*.parquet"
    export OUTPUT_GCS_PATH="gs://sangraha/pdfs_out_dedup_pdf/minhash/$lang"
    
    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --jars gs://spark-lib/bigquery/spark-3.3-bigquery-0.32.2.jar \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="50g","spark.driver.memory"="8g","spark.executor.cores"="7" \
        gs://sangraha/setu/text-dedup/minhash_spark.py \
        -- \
        --column "text" \
        --threshold "0.999" \
        --ngram_size "16" \
        --min_length "16" \
        --num_perm "2056" \
        --input $INPUT_GCS_PATH \
        --output $OUTPUT_GCS_PATH \
        --debug
done