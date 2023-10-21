#!/bin/usr/env bash

# for LANG in "Assamese" "Bengali" "Gujarati" "Hindi" "Kannada" "Malayalam" "Marathi" "Nepali" "Oriya" "Punjabi" "Sanskrit" "Tamil" "Telugu" "Urdu";
# for LANG in "Bengali" "Gujarati" "Hindi" "Kannada" "Malayalam" "Marathi" "Nepali" "Oriya" "Punjabi" "Sanskrit" "Tamil" "Telugu" "Urdu";
# for LANG in "Hindi";
# for LANG in "Gujarati";
# for LANG in "Sanskrit";
# "Bengali" "English" "Hindi" "Marathi" "Tamil" "Telugu" "Urdu"
# for LANG in "Assamese" "Bodo" "Dogri" "Gujarati" "Kannada" "Kashmiri" "Konkani" "Maithili";
for LANG in  "assamese" "bengali" "gujarati" "kannada" "konkani" "maithili" "malayalam" "marathi" "nepali" "oriya" "punjabi" "sanskrit" "tamil";
do
    CLUSTER_NAME="setu-debugging-15-highmem"
    # CLUSTER_NAME="setu-debugging-50-highmem"
    # CLUSTER_NAME="setu-debugging-15"
    # CLUSTER_NAME="setu-debugging"
    REGION="asia-south1"
    GS_SETU_ROOT="gs://sangraha/setu"
    # GS_INPUT_ROOT="gs://sangraha/pdfs_text"
    # GS_OUTPUT_ROOT="gs://sangraha/pdfs_out"
    # GS_INPUT_ROOT="gs://sangraha/parquets"
    # GS_INPUT_ROOT="gs://sangraha/spark_out"
    GS_INPUT_ROOT="gs://sangraha/crawls_raw_600"
    GS_OUTPUT_ROOT="gs://sangraha/spark_out_new"
    SAVE_LANG=`echo "$LANG" | tr '[:upper:]' '[:lower:]'`
    echo "Running for $SAVE_LANG"
    SHARD="*"
    OUTPUT_SHARD="dataset"

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="50g","spark.driver.memory"="8g","spark.executor.cores"="14","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config $GS_SETU_ROOT/dataproc/configs/spark_${SAVE_LANG}_config.json \
    #     --mode ocr \
    #     --run_local False \
    #     JSON2ParquetStage \
    #     --json_glob_path "$GS_INPUT_ROOT/$LANG/$SHARD/*/*.json" \
    #     --language "$SAVE_LANG" \
    #     --j2p_samples_per_partition 1500 \
    #     --j2p_verbose False \
    #     --j2p_run_mode data \
    #     --j2p_parquet_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$SHARD/json2parquets/dataset" \
    #     --j2p_bucket "sangraha" \
    #     --j2p_bucket_prefix "pdfs_text/$LANG/$SHARD/"

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="75g","spark.driver.memory"="50g","spark.executor.cores"="12","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/crawls/spark_${SAVE_LANG}_config.json" \
    #     --mode crawl \
    #     --run_local False \
    #     DocCleanStage \
    #     --doc_df_parquets_path "$GS_INPUT_ROOT/$SAVE_LANG/$SHARD/*.parquet" \
    #     --is_doc_df_path_batched False \
    #     --doc_clean_additional_cols_to_use "url,source,language" \
    #     --use_symbol_filter True \
    #     --doc_clean_samples_per_partition 1500 \
    #     --doc_clean_verbose False \
    #     --doc_clean_run_mode data \
    #     --save_symbol_heavy_docs True \
    #     --symbol_filter_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/cleaned_docs/symbol_heavy/dataset" \
    #     --cleaned_doc_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/cleaned_docs/cleaned_docs/dataset"

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/crawls/spark_${SAVE_LANG}_config.json" \
    #     --mode crawl \
    #     --run_local False \
    #     VisualizeDocCleanStage \
    #     --viz_cleaned_docs_parquets "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/cleaned_docs/cleaned_docs/dataset/*.parquet" \
    #     --viz_symbol_heavy_parquets "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/cleaned_docs/symbol_heavy/dataset/*.parquet" \
    #     --doc_clean_viz_output "$GS_OUTPUT_ROOT/$SAVE_LANG/visualization/$OUTPUT_SHARD/doc_clean" \
    #     --dc_viz_gcp_bucket "sangraha" \
    #     --viz_dc_stage_run_mode "stage"


    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="75g","spark.driver.memory"="50g","spark.executor.cores"="12","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/crawls/spark_${SAVE_LANG}_config.json" \
    #     --mode crawl \
    #     --run_local False \
    #     LIDStage \
    #     --lid_df_parquets_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/cleaned_docs/cleaned_docs/dataset/*.parquet" \
    #     --is_lid_df_path_batched False \
    #     --lid_additional_cols "url,source,language" \
    #     --lid_samples_per_partition 1500 \
    #     --lid_verbose True \
    #     --lid_run_mode data \
    #     --doc_lid_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/lid_segregation/doc_lid/dataset"

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/crawls/spark_${SAVE_LANG}_config.json" \
    #     --mode crawl \
    #     --run_local False \
    #     VisualizeLIDStage \
    #     --lid_parquets "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/lid_segregation/doc_lid/dataset/*/*.parquet" \
    #     --cleaned_docs_parquets "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/cleaned_docs/cleaned_docs/dataset/*.parquet" \
    #     --lid_viz_output "$GS_OUTPUT_ROOT/$SAVE_LANG/visualization/$OUTPUT_SHARD/lid" \
    #     --lid_viz_gcp_bucket "sangraha" \
    #     --viz_lid_stage_run_mode "stage" \
    #     --viz_lid_perform_join True

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="75g","spark.driver.memory"="50g","spark.executor.cores"="12","spark.submit.deployMode"="client" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config "$GS_SETU_ROOT/dataproc/configs/crawls/spark_${SAVE_LANG}_config.json" \
        --mode crawl \
        --run_local False \
        AnalysisStage \
        --analysis_df_parquets_path "$GS_INPUT_ROOT/$SAVE_LANG/*/lid_segregation/doc_lid/*/*/*.parquet" \
        --is_analysis_df_path_batched False \
        --analysis_additional_cols_to_use "doc_lang,doc_lang_iso" \
        --analysis_samples_per_partition 1500 \
        --analysis_verbose False \
        --analysis_run_mode stage \
        --line_stats_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/analysis/line_out/dataset" \
        --doc_stats_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/analysis/doc_stats_out/dataset" \
        --analysis_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/analysis/analysis/dataset"

        # --analysis_additional_cols_to_use "doc_lang,doc_lang_iso,url,source,language" \

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/crawls/spark_${SAVE_LANG}_config.json" \
    #     --mode crawl \
    #     --run_local False \
    #     VisualizeAnalysisStage \
    #     --analysis_doc_stats_parquets "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/analysis/doc_stats_out/dataset/*/*.parquet" \
    #     --analysis_viz_output "$GS_OUTPUT_ROOT/$SAVE_LANG/visualization/$OUTPUT_SHARD/analysis" \
    #     --ana_viz_language "$SAVE_LANG" \
    #     --ana_viz_gcp_bucket "sangraha" \
    #     --viz_analysis_stage_run_mode "stage"

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/crawls/spark_${SAVE_LANG}_config.json" \
    #     --mode crawl \
    #     --run_local False \
    #     FlaggingAndFilteringStage \
    #     --doc_stats_parquets_path "$GS_OUTPUT_ROOT/$SAVE_LANG/dataset/analysis/doc_stats_out/dataset/doc_lang_partition=$SAVE_LANG/*.parquet" \
    #     --is_doc_stats_path_batched False \
    #     --fnf_samples_per_partition 1500 \
    #     --fnf_verbose False \
    #     --fnf_run_mode stage \
    #     --save_nsfw_data True \
    #     --nsfw_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/dataset/filtering/nsfw_doc_stats/dataset" \
    #     --filtered_doc_stats_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/dataset/filtering/filtered_doc_stats/dataset"

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/crawls/spark_${SAVE_LANG}_config.json" \
    #     --mode crawl \
    #     --run_local False \
    #     DocumentRemovalStage \
    #     --analysis_out_path "$GS_OUTPUT_ROOT/$SAVE_LANG/dataset/analysis/analysis/dataset/doc_lang_partition=$SAVE_LANG/*.parquet" \
    #     --doc_stats_path "$GS_OUTPUT_ROOT/$SAVE_LANG/dataset/filtering/filtered_doc_stats/dataset/*.parquet" \
    #     --doc_removal_samples_per_partition 1500 \
    #     --doc_removal_verbose False \
    #     --doc_removal_run_mode stage \
    #     --filtered_docs_path "$GS_OUTPUT_ROOT/$SAVE_LANG/dataset/filtered_docs/filtered_docs/dataset"

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/crawls/spark_${SAVE_LANG}_config.json" \
    #     --mode crawl \
    #     --run_local False \
    #     VisualizeFilterStage \
    #     --filtered_docs_parquets "$GS_OUTPUT_ROOT/$SAVE_LANG/dataset/filtered_docs/filtered_docs/dataset/*.parquet" \
    #     --filter_viz_output "$GS_OUTPUT_ROOT/$SAVE_LANG/visualization/dataset/filter" \
    #     --fil_viz_gcp_bucket sangraha \
    #     --viz_filter_stage_run_mode stage

done
