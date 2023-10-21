#!/bin/usr/env bash

# "Bengali" "Gujarati" 
# for LANG in "Assamese";
for LANG in "Nepali" "Oriya" "Punjabi" "Sanskrit" "Tamil" "Telugu" "Urdu";
do
    CLUSTER_NAME="setu-debugging-50-highmem"
    # CLUSTER_NAME="setu-debugging-15-highmem"
    # CLUSTER_NAME="setu-debugging-5-highmem"
    REGION="asia-south1"
    GS_SETU_ROOT="gs://sangraha/setu"
    # GS_INPUT_ROOT="gs://sangraha/pdfs_text"
    # GS_INPUT_ROOT="gs://sangraha/pdf_texts_debugged"
    GS_INPUT_ROOT="gs://sangraha/pdfs_out"
    GS_OUTPUT_ROOT="gs://sangraha/pdfs_out_full"
    SAVE_LANG=`echo "$LANG" | tr '[:upper:]' '[:lower:]'`
    echo "Running for $SAVE_LANG"
    SHARD="*"
    OUTPUT_SHARD="1_2_3_4"

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config $GS_SETU_ROOT/dataproc/configs/spark_${SAVE_LANG}_config.json \
    #     --mode ocr \
    #     --run_local False \
    #     JSON2ParquetStage \
    #     --json_glob_path "$GS_INPUT_ROOT/$LANG/$SHARD/*.jsonl" \
    #     --j2p_cols "doc_id,url,source,page_no,identifier,pdf_name,text" \
    #     --language "$SAVE_LANG" \
    #     --j2p_samples_per_partition 1500 \
    #     --j2p_verbose False \
    #     --j2p_run_mode stage \
    #     --j2p_parquet_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/json2parquets/dataset" \
    #     --j2p_bucket "sangraha" \
    #     --j2p_bucket_prefix "pdfs_text/$LANG/$SHARD/"

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/spark_${SAVE_LANG}_config.json" \
    #     --mode crawl \
    #     --run_local False \
    #     VisualizeJson2ParquetStage \
    #     --viz_j2p_parquets "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/json2parquets/dataset/*.parquet" \
    #     --j2p_viz_output "$GS_OUTPUT_ROOT/$SAVE_LANG/visualization/$OUTPUT_SHARD/json2parquet" \
    #     --j2p_viz_gcp_bucket "sangraha" \
    #     --viz_j2p_stage_run_mode "stage"

    # HERRRRRRRRREEEEEEEEEEEEEEEEEEEEEEE

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/ocr/spark_${SAVE_LANG}_config.json" \
    #     --mode crawl \
    #     --run_local False \
    #     DocCleanStage \
    #     --doc_df_parquets_path "$GS_INPUT_ROOT/$SAVE_LANG/$SHARD/json2parquets/dataset/*.parquet" \
    #     --is_doc_df_path_batched False \
    #     --doc_clean_additional_cols_to_use "url,source,page_no,language" \
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
    #     --config "$GS_SETU_ROOT/dataproc/configs/ocr/spark_${SAVE_LANG}_config.json" \
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
    #     --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/ocr/spark_${SAVE_LANG}_config.json" \
    #     --mode ocr \
    #     --run_local False \
    #     Page2PDFStage \
    #     --p2p_parquets_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/cleaned_docs/cleaned_docs/dataset/*.parquet" \
    #     --p2p_doc_stats_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/cleaned_docs/cleaned_docs/dataset/*.parquet" \
    #     --p2p_samples_per_partition 1500 \
    #     --p2p_data_quality "uncleaned" \
    #     --p2p_text_col "uncleaned_text" \
    #     --p2p_lang_col None \
    #     --p2p_identifier_col "doc_id" \
    #     --p2p_groupby_col "url" \
    #     --p2p_sort_col "page_no" \
    #     --p2p_symbol_join "" \
    #     --p2p_doc_stats_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/page2pdfs_uncleaned/pdf_stats/dataset" \
    #     --p2p_doc_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/page2pdfs_uncleaned/page2pdfs/dataset"

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/ocr/spark_${SAVE_LANG}_config.json" \
    #     --mode crawl \
    #     --run_local False \
    #     LIDStage \
    #     --lid_df_parquets_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/cleaned_docs/cleaned_docs/dataset/*.parquet" \
    #     --is_lid_df_path_batched False \
    #     --lid_additional_cols "url,source,page_no,language" \
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
    #     --config "$GS_SETU_ROOT/dataproc/configs/ocr/spark_${SAVE_LANG}_config.json" \
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
        --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config "$GS_SETU_ROOT/dataproc/configs/ocr/spark_${SAVE_LANG}_config.json" \
        --mode ocr \
        --run_local False \
        AnalysisStage \
        --analysis_df_parquets_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/lid_segregation/doc_lid/dataset/*/*.parquet" \
        --is_analysis_df_path_batched False \
        --analysis_additional_cols_to_use "doc_lang,doc_lang_iso,url,source,page_no,language" \
        --analysis_samples_per_partition 1500 \
        --analysis_verbose False \
        --analysis_run_mode stage \
        --line_stats_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/analysis/line_out/dataset" \
        --doc_stats_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/analysis/doc_stats_out/dataset" \
        --analysis_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/analysis/analysis/dataset"

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config "$GS_SETU_ROOT/dataproc/configs/ocr/spark_${SAVE_LANG}_config.json" \
        --mode crawl \
        --run_local False \
        VisualizeAnalysisStage \
        --analysis_doc_stats_parquets "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/analysis/doc_stats_out/dataset/*/*.parquet" \
        --analysis_viz_output "$GS_OUTPUT_ROOT/$SAVE_LANG/visualization/$OUTPUT_SHARD/analysis" \
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
        --config "$GS_SETU_ROOT/dataproc/configs/ocr/spark_${SAVE_LANG}_config.json" \
        --mode ocr \
        --run_local False \
        Page2PDFStage \
        --p2p_parquets_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/analysis/analysis/dataset/*/*.parquet" \
        --p2p_doc_stats_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/analysis/doc_stats_out/dataset/*/*.parquet" \
        --p2p_samples_per_partition 1500 \
        --p2p_data_quality "cleaned" \
        --p2p_text_col "text" \
        --p2p_lang_col "doc_lang" \
        --p2p_identifier_col "doc_id" \
        --p2p_groupby_col "url" \
        --p2p_sort_col "page_no" \
        --p2p_symbol_join "" \
        --p2p_doc_stats_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/page2pdfs/pdf_stats/dataset" \
        --p2p_doc_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/page2pdfs/page2pdfs/dataset"

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/ocr/spark_${SAVE_LANG}_config.json" \
    #     --mode crawl \
    #     --run_local False \
    #     FlaggingAndFilteringStage \
    #     --doc_stats_parquets_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/analysis/doc_stats_out/dataset/doc_lang_partition=$SAVE_LANG/*.parquet" \
    #     --is_doc_stats_path_batched False \
    #     --fnf_samples_per_partition 1500 \
    #     --fnf_verbose False \
    #     --fnf_run_mode stage \
    #     --save_nsfw_data True \
    #     --nsfw_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/filtering/nsfw_doc_stats/dataset" \
    #     --filtered_doc_stats_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/filtering/filtered_doc_stats/dataset"

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/ocr/spark_${SAVE_LANG}_config.json" \
    #     --mode crawl \
    #     --run_local False \
    #     DocumentRemovalStage \
    #     --analysis_out_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/analysis/analysis/dataset/doc_lang_partition=$SAVE_LANG/*.parquet" \
    #     --doc_stats_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/filtering/filtered_doc_stats/dataset/*.parquet" \
    #     --doc_removal_samples_per_partition 1500 \
    #     --doc_removal_verbose False \
    #     --doc_removal_run_mode stage \
    #     --filtered_docs_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/filtered_docs/filtered_docs/dataset"

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config "$GS_SETU_ROOT/dataproc/configs/ocr/spark_${SAVE_LANG}_config.json" \
    #     --mode crawl \
    #     --run_local False \
    #     VisualizeFilterStage \
    #     --filtered_docs_parquets "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD/filtered_docs/filtered_docs/dataset/*.parquet" \
    #     --filter_viz_output "$GS_OUTPUT_ROOT/$SAVE_LANG/visualization/$OUTPUT_SHARD/filter" \
    #     --fil_viz_gcp_bucket sangraha \
    #     --viz_filter_stage_run_mode stage

done
