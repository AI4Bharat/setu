#!/bin/usr/env bash

LANG=$1
PIPELINE=$2
SHARD=$3



if [ $PIPELINE == "minhash" ]; then

    export CLUSTER_NAME=minhash-30
    export PROJECT_ID="sangraha-396106"
    export REGION=asia-south1
    export ZONE=asia-south1-a

    export INPUT_GCS_PATH="gs://sangraha/pdfs_out_full/$LANG/*/filtered_docs/filtered_docs/dataset/*.parquet"
    export OUTPUT_GCS_PATH="gs://sangraha/pdfs_out_dedup_full/minhash/$LANG"
    
    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME}\
        --region $REGION \
        --jars gs://spark-lib/bigquery/spark-3.3-bigquery-0.32.2.jar \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="50g","spark.driver.memory"="8g","spark.executor.cores"="7" \
        gs://sangraha/setu/text-dedup/minhash_spark.py \
        -- \
        --column "text" \
        --threshold "0.999" \
        --ngram_size "5" \
        --min_length "5" \
        --num_perm "1024" \
        --input $INPUT_GCS_PATH \
        --output $OUTPUT_GCS_PATH \
        --debug
fi

if [ $PIPELINE == "clean" ]; then

    # CLUSTER_NAME="setu-debugging-15-highmem"
    CLUSTER_NAME="setu-debugging-50-highmem"
    # CLUSTER_NAME="setu-debugging-15"
    # CLUSTER_NAME="setu-debugging"
    REGION="asia-south1"
    GS_SETU_ROOT="gs://sangraha/setu"
    GS_INPUT_ROOT="gs://sangraha/pdfs_text"
    GS_OUTPUT_ROOT="gs://sangraha/pdfs_out"
    SAVE_LANG=`echo "$LANG" | tr '[:upper:]' '[:lower:]'`
    echo "Running for $SAVE_LANG"

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="50g","spark.driver.memory"="8g","spark.executor.cores"="14","spark.submit.deployMode"="client" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config $GS_SETU_ROOT/dataproc/configs/spark_${SAVE_LANG}_config.json \
        --mode ocr \
        --run_local False \
        JSON2ParquetStage \
        --json_glob_path "$GS_INPUT_ROOT/$LANG/$SHARD/*.jsonl" \
        --language "$SAVE_LANG" \
        --j2p_samples_per_partition 1500 \
        --j2p_verbose False \
        --j2p_run_mode stage \
        --j2p_parquet_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/json2parquets/dataset" \
        --j2p_bucket "sangraha" \
        --j2p_bucket_prefix "pdfs_text/$LANG/$SHARD/"

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="50g","spark.driver.memory"="8g","spark.executor.cores"="14","spark.submit.deployMode"="client" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config $GS_SETU_ROOT/dataproc/configs/spark_${SAVE_LANG}_config.json \
        --mode ocr \
        --run_local False \
        JSON2ParquetStage \
        --json_glob_path "$GS_INPUT_ROOT/$LANG/$SHARD/*/*.json" \
        --language "$SAVE_LANG" \
        --j2p_samples_per_partition 1500 \
        --j2p_verbose False \
        --j2p_run_mode data \
        --j2p_parquet_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/json2parquets/dataset" \
        --j2p_bucket "sangraha" \
        --j2p_bucket_prefix "pdfs_text/$LANG/$SHARD/"

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config "$GS_SETU_ROOT/dataproc/configs/spark_${SAVE_LANG}_config.json" \
        --mode crawl \
        --run_local False \
        DocCleanStage \
        --doc_df_parquets_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/json2parquets/dataset/*.parquet" \
        --is_doc_df_path_batched False \
        --doc_clean_cols_to_use "doc_id,url,source,text,language" \
        --use_symbol_filter True \
        --doc_clean_samples_per_partition 1500 \
        --doc_clean_verbose False \
        --doc_clean_run_mode data \
        --save_symbol_heavy_docs True \
        --symbol_filter_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/cleaned_docs/symbol_heavy/dataset" \
        --cleaned_doc_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/cleaned_docs/cleaned_docs/dataset"

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config "$GS_SETU_ROOT/dataproc/configs/spark_${SAVE_LANG}_config.json" \
        --mode crawl \
        --run_local False \
        LIDStage \
        --lid_df_parquets_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/cleaned_docs/cleaned_docs/dataset/*.parquet" \
        --is_lid_df_path_batched False \
        --lid_samples_per_partition 1500 \
        --lid_verbose True \
        --lid_run_mode data \
        --doc_lid_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/lid_segregation/doc_lid/dataset"

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config "$GS_SETU_ROOT/dataproc/configs/spark_${SAVE_LANG}_config.json" \
        --mode crawl \
        --run_local False \
        AnalysisStage \
        --analysis_df_parquets_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/lid_segregation/doc_lid/dataset/*/*.parquet" \
        --is_analysis_df_path_batched False \
        --analysis_samples_per_partition 1500 \
        --analysis_verbose False \
        --analysis_run_mode stage \
        --line_stats_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/analysis/line_out/dataset" \
        --doc_stats_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/analysis/doc_stats_out/dataset" \
        --analysis_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/analysis/analysis/dataset"

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config "$GS_SETU_ROOT/dataproc/configs/spark_${SAVE_LANG}_config.json" \
        --mode crawl \
        --run_local False \
        FlaggingAndFilteringStage \
        --doc_stats_parquets_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/analysis/doc_stats_out/dataset/doc_lang_partition=$SAVE_LANG/*.parquet" \
        --is_doc_stats_path_batched False \
        --fnf_samples_per_partition 1500 \
        --fnf_verbose False \
        --fnf_run_mode stage \
        --save_nsfw_data True \
        --nsfw_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/filtering/nsfw_doc_stats/dataset" \
        --filtered_doc_stats_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/filtering/filtered_doc_stats/dataset"

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config "$GS_SETU_ROOT/dataproc/configs/spark_${SAVE_LANG}_config.json" \
        --mode crawl \
        --run_local False \
        DocumentRemovalStage \
        --analysis_out_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/analysis/analysis/dataset/doc_lang_partition=$SAVE_LANG/*.parquet" \
        --doc_stats_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/filtering/filtered_doc_stats/dataset/*.parquet" \
        --doc_removal_samples_per_partition 1500 \
        --doc_removal_verbose False \
        --doc_removal_run_mode stage \
        --filtered_docs_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/filtered_docs/filtered_docs/dataset"

fi

if [ $PIPELINE == "visualize" ]; then

    # CLUSTER_NAME="setu-debugging-15-highmem"
    CLUSTER_NAME="setu-debugging-50-highmem"
    # CLUSTER_NAME="setu-debugging-15"
    # CLUSTER_NAME="setu-debugging"
    REGION="asia-south1"
    GS_SETU_ROOT="gs://sangraha/setu"
    GS_INPUT_ROOT="gs://sangraha/pdfs_text"
    GS_OUTPUT_ROOT="gs://sangraha/pdfs_out"
    SAVE_LANG=`echo "$LANG" | tr '[:upper:]' '[:lower:]'`
    echo "Running for $SAVE_LANG"

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config "$GS_SETU_ROOT/dataproc/configs/spark_${SAVE_LANG}_config.json" \
        --mode crawl \
        --run_local False \
        VisualizeJson2ParquetStage \
        --viz_j2p_parquets "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/json2parquets/dataset/*.parquet" \
        --j2p_viz_output "$GS_OUTPUT_ROOT/$SAVE_LANG/visualization/$SHARD/json2parquet" \
        --j2p_viz_gcp_bucket "sangraha" \
        --viz_j2p_stage_run_mode "stage"

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config "$GS_SETU_ROOT/dataproc/configs/spark_${SAVE_LANG}_config.json" \
        --mode crawl \
        --run_local False \
        VisualizeDocCleanStage \
        --viz_cleaned_docs_parquets "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/cleaned_docs/cleaned_docs/dataset/*.parquet" \
        --viz_symbol_heavy_parquets "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/cleaned_docs/symbol_heavy/dataset/*.parquet" \
        --doc_clean_viz_output "$GS_OUTPUT_ROOT/$SAVE_LANG/visualization/$SHARD/doc_clean" \
        --dc_viz_gcp_bucket "sangraha" \
        --viz_dc_stage_run_mode "stage"

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config "$GS_SETU_ROOT/dataproc/configs/spark_${SAVE_LANG}_config.json" \
        --mode crawl \
        --run_local False \
        VisualizeLIDStage \
        --lid_parquets "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/lid_segregation/doc_lid/dataset/*/*.parquet" \
        --cleaned_docs_parquets "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/cleaned_docs/cleaned_docs/dataset/*.parquet" \
        --lid_viz_output "$GS_OUTPUT_ROOT/$SAVE_LANG/visualization/$SHARD/lid" \
        --lid_viz_gcp_bucket "sangraha" \
        --viz_lid_stage_run_mode "stage" \
        --viz_lid_perform_join True

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config "$GS_SETU_ROOT/dataproc/configs/spark_${SAVE_LANG}_config.json" \
        --mode crawl \
        --run_local False \
        VisualizeAnalysisStage \
        --analysis_doc_stats_parquets "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/analysis/doc_stats_out/dataset/*/*.parquet" \
        --analysis_viz_output "$GS_OUTPUT_ROOT/$SAVE_LANG/visualization/$SHARD/analysis" \
        --ana_viz_language "$SAVE_LANG" \
        --ana_viz_gcp_bucket "sangraha" \
        --viz_analysis_stage_run_mode "stage"

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config "$GS_SETU_ROOT/dataproc/configs/spark_${SAVE_LANG}_config.json" \
        --mode crawl \
        --run_local False \
        VisualizeFilterStage \
        --filtered_docs_parquets "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/filtered_docs/filtered_docs/dataset/*.parquet" \
        --filter_viz_output "$GS_OUTPUT_ROOT/$SAVE_LANG/visualization/$SHARD/filter" \
        --fil_viz_gcp_bucket sangraha \
        --viz_filter_stage_run_mode stage

fi

