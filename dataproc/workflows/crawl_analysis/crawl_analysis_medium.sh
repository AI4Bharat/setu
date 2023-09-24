#!/bin/bash

# for lang in "gujarati" "kannada" "oriya" "punjabi" "sindhi"; do

for lang in "gujarati"; do

    gcloud dataproc workflow-templates instantiate crawl_analysis_cluster_selector_template \
        --region="asia-south1" \
        --parameters="CLUSTER_NAME=flagging-and-filtering-medium,MAIN_PYTHON_FILE=gs://sangraha/setu/run.py,CONFIG=gs://sangraha/setu/dataproc/configs/spark_${lang}_config.json,SAMPLES_PER_PARTITION=18000,VERBOSE=False,CHECKPOINT_DIR=gs://sangraha/lid_checkpoint,RUN_DATA_PARALLEL_MODE=True,DOC_DF_PARQUETS_PATH=gs://sangraha/parquets/$lang/*/*.parquet,IS_DOC_DF_PATH_BATCHED=False,USE_SYMBOL_FILTER=True,SAVE_SYMBOL_HEAVY_DOCS=True,SYMBOL_FILTER_OUTPUT_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/cleaned_docs/symbol_heavy/dataset,CLEANED_DOC_OUTPUT_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/cleaned_docs/cleaned_docs/dataset,LID_DF_PARQUETS_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/cleaned_docs/cleaned_docs/*/*/*.parquet,IS_LID_DF_PATH_BATCHED=False,DOC_LID_OUTPUT_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/lid_segregation/doc_lid/dataset,ANALYSIS_DF_PARQUETS_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/lid_segregation/doc_lid/*/*/*.parquet,IS_ANALYSIS_DF_PATH_BATCHED=False,LINE_STATS_OUTPUT_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/analysis/line_stats/dataset,DOC_STATS_OUTPUT_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/analysis/doc_lid/dataset,ANALYSIS_OUTPUT_PATH=gs://sangraha/spark_out_dataproc/$lang/dataset/analysis/analysis/dataset,SPARK_PARALLELISM=48,SPARK_SHUFFLE_PARTITION_COUNT=512,ENBLE_ARROW_EXECUTION=True,ENABLE_ADAPTIVE_SQL=True,SPARK_SERIALIZER=org.apache.spark.serializer.KryoSerializer,ENABLE_SPECULATION=True"

done