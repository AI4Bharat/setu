#!/bin/usr/env bash

CLUSTER_NAME=setu-debugging-50-highmem
REGION=asia-south1
GS_SETU_ROOT="gs://sangraha/setu"
GS_OUTPUT_ROOT="gs://sangraha/spark_out_test"

extract_stats() {

    lang=$1
    symbol_heavy_parquets=$2
    lid_parquets=$3
    cleaned_doc_parquets=$4
    doc_stats_parquets=$5
    filtered_doc_parquets=$6
    perform_join=$7

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="50g","spark.driver.memory"="30g","spark.executor.cores"="12","spark.submit.deployMode"="client" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config "$GS_SETU_ROOT/dataproc/configs/spark_${lang}_config.json" \
        --mode ocr \
        --run_local False \
        VisualizeDocCleanStage \
        --viz_cleaned_docs_parquets "$GS_OUTPUT_ROOT/$lang/*/$cleaned_doc_parquets/*/*.parquet" \
        --viz_symbol_heavy_parquets "$GS_OUTPUT_ROOT/$lang/*/$symbol_heavy_parquets/*/*.parquet" \
        --doc_clean_viz_output "$GS_OUTPUT_ROOT/$lang/visualization/dataset/doc_clean" \
        --dc_viz_gcp_bucket "sangraha" \
        --viz_dc_stage_run_mode "stage"

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="50g","spark.driver.memory"="30g","spark.executor.cores"="12","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/spark_${lang}_config.json" \
    #     --mode ocr \
    #     --run_local False \
    #     VisualizeLIDStage \
    #     --lid_parquets "$GS_OUTPUT_ROOT/$lang/*/$lid_parquets/*/*/*.parquet" \
    #     --cleaned_docs_parquets "$GS_OUTPUT_ROOT/$lang/*/$cleaned_doc_parquets/*/*.parquet" \
    #     --lid_viz_output "$GS_OUTPUT_ROOT/$lang/visualization/dataset/lid" \
    #     --lid_viz_gcp_bucket "sangraha" \
    #     --viz_lid_stage_run_mode "stage" \
    #     --viz_lid_perform_join $perform_join

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="50g","spark.driver.memory"="30g","spark.executor.cores"="12","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/spark_${lang}_config.json" \
    #     --mode ocr \
    #     --run_local False \
    #     VisualizeAnalysisStage \
    #     --analysis_doc_stats_parquets "$GS_OUTPUT_ROOT/$lang/*/$doc_stats_parquets/*/*/*.parquet" \
    #     --analysis_viz_output "$GS_OUTPUT_ROOT/$lang/visualization/dataset/analysis" \
    #     --ana_viz_language "$lang" \
    #     --ana_viz_gcp_bucket "sangraha" \
    #     --viz_analysis_stage_run_mode "stage"

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="50g","spark.driver.memory"="30g","spark.executor.cores"="12","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/spark_${lang}_config.json" \
    #     --mode ocr \
    #     --run_local False \
    #     VisualizeFilterStage \
    #     --filtered_docs_parquets "$GS_OUTPUT_ROOT/$lang/*/$filtered_doc_parquets/*/*.parquet" \
    #     --filter_viz_output "$GS_OUTPUT_ROOT/$lang/visualization/dataset/filter" \
    #     --fil_viz_gcp_bucket sangraha \
    #     --viz_filter_stage_run_mode stage

}

# for lang in "assamese" "bodo" "dogri" "gujarati" "kannada" "kashmiri" "konkani" "maithili" "manipuri" "nepali" "oriya" "punjabi" "santhali" "sindhi";
# do
#     symbol_heavy_parquets="lid_segregation/symbol_heavy"
#     lid_parquets="lid_segregation/doc_lid"
#     cleaned_doc_parquets="lid_segregation/doc_lid"
#     doc_stats_parquets="analysis/doc_stats"
#     filtered_doc_parquets="filtered_docs/filtered_docs"

#     extract_stats $lang $symbol_heavy_parquets $lid_parquets $cleaned_doc_parquets $doc_stats_parquets $filtered_doc_parquets False >> stats_logs/$lang.txt
# done

# for lang in "bengali" "english" "hindi" "malayalam" "marathi" "sanskrit" "tamil" "telugu" "urdu";
# do 
#     symbol_heavy_parquets="cleaned_docs/symbol_heavy"
#     lid_parquets="lid_segregation/doc_lid"
#     cleaned_doc_parquets="cleaned_docs/cleaned_docs"
#     doc_stats_parquets="analysis/doc_stats"
#     filtered_doc_parquets="filtered_docs/filtered_docs"

#     extract_stats $lang $symbol_heavy_parquets $lid_parquets $cleaned_doc_parquets $doc_stats_parquets $filtered_doc_parquets True >> stats_logs/$lang.txt
# done

for lang in "assamese" "bengali" "bodo" "dogri" "english" "gujarati" "hindi" "kannada" "kashmiri" "konkani" "maithili" "malayalam" "manipuri" "marathi" "nepali" "oriya" "punjabi" "sanskrit" "santhali" "sindhi" "tamil" "telugu" "urdu";
do 
    symbol_heavy_parquets="cleaned_docs/symbol_heavy"
    lid_parquets="lid_segregation/doc_lid"
    cleaned_doc_parquets="cleaned_docs/cleaned_docs"
    doc_stats_parquets="analysis/doc_stats"
    filtered_doc_parquets="filtered_docs/filtered_docs"

    extract_stats $lang $symbol_heavy_parquets $lid_parquets $cleaned_doc_parquets $doc_stats_parquets $filtered_doc_parquets True >> stats_logs/$lang.txt
done