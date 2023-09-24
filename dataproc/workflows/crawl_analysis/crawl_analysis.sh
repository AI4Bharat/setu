#!/bin/bash

for lang in "assamese" "bodo" "dogri" "kashmiri" "konkani" "maithili" "manipuri" "sanskrit" "santhali"; do

    gcloud dataproc workflow-templates instantiate flagging_and_filtering_low_resource_template \
        --region="asia-south1" \
        --parameters="CLUSTER_NAME=$lang-flagging-filtering,MAIN_PYTHON_FILE=gs://sangraha/setu/run.py,CONFIG=gs://sangraha/setu/dataproc/configs/spark_${lang}_config.json,SAMPLES_PER_PARTITION=18000,VERBOSE=False,ANALYSIS_DF_PARQUETS_PATH=gs://sangraha/spark_out/*/*/analysis/analysis/*/$lang/*.parquet,DOC_STATS_PARQUETS_PATH=gs://sangraha/spark_out/*/*/analysis/doc_stats/*/$lang/*.parquet,IS_DOC_STATS_PATH_BATCHED=False,SAVE_NSFW_DATA=True,NSFW_OUTPUT_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/filtering/nsfw_doc_stats/dataset,FILTERED_DOC_STATS_OUTPUT_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/filtering/filtered_doc_stats/dataset,DOC_STATS_PATH_FOR_REMOVAL=gs://sangraha/spark_out_dataproc/$lang/dataset/filtering/filtered_doc_stats/*/*.parquet,FILTERED_DOCS_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/filtered_docs/filtered_docs/dataset,SPARK_PARALLELISM=48,SPARK_SHUFFLE_PARTITION_COUNT=512,ENBLE_ARROW_EXECUTION=True,ENABLE_ADAPTIVE_SQL=True,SPARK_SERIALIZER=org.apache.spark.serializer.KryoSerializer,ENABLE_SPECULATION=True"

done

for lang in "gujarati" "kannada" "oriya" "punjabi" "sindhi"; do

    gcloud dataproc workflow-templates instantiate flagging_and_filtering_medium_resource_template \
        --region="asia-south1" \
        --parameters="CLUSTER_NAME=$lang-flagging-filtering,MAIN_PYTHON_FILE=gs://sangraha/setu/run.py,CONFIG=gs://sangraha/setu/dataproc/configs/spark_${lang}_config.json,SAMPLES_PER_PARTITION=18000,VERBOSE=False,ANALYSIS_DF_PARQUETS_PATH=gs://sangraha/spark_out/*/*/analysis/analysis/*/$lang/*.parquet,DOC_STATS_PARQUETS_PATH=gs://sangraha/spark_out/*/*/analysis/doc_stats/*/$lang/*.parquet,IS_DOC_STATS_PATH_BATCHED=False,SAVE_NSFW_DATA=True,NSFW_OUTPUT_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/filtering/nsfw_doc_stats/dataset,FILTERED_DOC_STATS_OUTPUT_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/filtering/filtered_doc_stats/dataset,DOC_STATS_PATH_FOR_REMOVAL=gs://sangraha/spark_out_dataproc/$lang/dataset/filtering/filtered_doc_stats/*/*.parquet,FILTERED_DOCS_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/filtered_docs/filtered_docs/dataset,SPARK_PARALLELISM=48,SPARK_SHUFFLE_PARTITION_COUNT=512,ENBLE_ARROW_EXECUTION=True,ENABLE_ADAPTIVE_SQL=True,SPARK_SERIALIZER=org.apache.spark.serializer.KryoSerializer,ENABLE_SPECULATION=True"

done

for lang in "bengali" "english" "hindi" "malayalam" "marathi" "nepali" "tamil" "telugu" "urdu"; do

    gcloud dataproc workflow-templates instantiate flagging_and_filtering_high_resource_template \
        --region="asia-south1" \
        --parameters="CLUSTER_NAME=$lang-flagging-filtering,MAIN_PYTHON_FILE=gs://sangraha/setu/run.py,CONFIG=gs://sangraha/setu/dataproc/configs/spark_${lang}_config.json,SAMPLES_PER_PARTITION=18000,VERBOSE=False,ANALYSIS_DF_PARQUETS_PATH=gs://sangraha/spark_out/*/*/analysis/analysis/*/$lang/*.parquet,DOC_STATS_PARQUETS_PATH=gs://sangraha/spark_out/*/*/analysis/doc_stats/*/$lang/*.parquet,IS_DOC_STATS_PATH_BATCHED=False,SAVE_NSFW_DATA=True,NSFW_OUTPUT_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/filtering/nsfw_doc_stats/dataset,FILTERED_DOC_STATS_OUTPUT_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/filtering/filtered_doc_stats/dataset,DOC_STATS_PATH_FOR_REMOVAL=gs://sangraha/spark_out_dataproc/$lang/dataset/filtering/filtered_doc_stats/*/*.parquet,FILTERED_DOCS_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/filtered_docs/filtered_docs/dataset,SPARK_PARALLELISM=48,SPARK_SHUFFLE_PARTITION_COUNT=512,ENBLE_ARROW_EXECUTION=True,ENABLE_ADAPTIVE_SQL=True,SPARK_SERIALIZER=org.apache.spark.serializer.KryoSerializer,ENABLE_SPECULATION=True"

done