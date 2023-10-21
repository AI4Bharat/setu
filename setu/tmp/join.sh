    
# for LANG in "Bengali" "Bodo" "Dogri" "English" "Gujarati" "Hindi" "Kannada" "Kashmiri" "Konkani" "Maithili" "Malayalam" "Manipuri" "Marathi" "Nepali" "Oriya" "Punjabi" "Sanskrit" "Sindhi" "Tamil" "Telugu" "Urdu";
for LANG in "Bengali" "Gujarati" "Hindi" "Kannada" "Malayalam" "Marathi" "Nepali" "Oriya" "Punjabi" "Sanskrit" "Tamil" "Telugu" 'Urdu';
# for LANG in "Assamese";
do  
    CLUSTER_NAME="setu-debugging-5-highmem"
    REGION="asia-south1"
    GS_SETU_ROOT="gs://sangraha/setu"
    # GS_INPUT_ROOT="gs://sangraha/parquets"
    # GS_INPUT_ROOT="gs://sangraha/pdfs_text"
    # GS_INPUT_ROOT="gs://sangraha/pdf_texts_debugged"
    # GS_OUTPUT_ROOT="gs://sangraha-out/crawls_lidscript"
    # GS_OUTPUT_ROOT="gs://sangraha/pdfs_out"
    SAVE_LANG=`echo "$LANG" | tr '[:upper:]' '[:lower:]'`
    echo "Running for $SAVE_LANG"

    # gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
    #     --region $REGION \
    #     --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
    #     --driver-log-levels root=FATAL,__main__=DEBUG \
    #     --properties="spark.executor.memory"="40g","spark.driver.memory"="50g","spark.executor.cores"="7","spark.submit.deployMode"="client" \
    #     gs://sangraha/setu/setu/run.py \
    #     -- \
    #     --config $GS_SETU_ROOT/dataproc/configs/spark_${SAVE_LANG}_config.json \
    #     --mode crawl \
    #     --run_local False \
    #     CustomJoinStage \
    #     --primary_parquets_path "gs://sangraha-out/crawls/$SAVE_LANG" \
    #     --columns_from_primary_parquets "*" \
    #     --secondary_parquets_path "gs://sangraha/spark_out/$SAVE_LANG/dataset/lid_segregation/doc_lid/*/$SAVE_LANG/*.parquet" \
    #     --columns_from_secondary_parquets "indiclid_code" \
    #     --identifier_cols "doc_id" \
    #     --join_type "inner" \
    #     --custom_join_run_mode "stage" \
    #     --joined_output_path "gs://sangraha/minhash_join_lidscript_out/$SAVE_LANG"

    # --secondary_parquets_path "gs://sangraha/spark_out/$SAVE_LANG/dataset/lid_segregation/doc_lid/*/doc_lang_partition=$SAVE_LANG/*.parquet" \

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
    #     CustomJoinStage \
    #     --primary_parquets_path "gs://sangraha/pdfs_out_dedup/minhash/$SAVE_LANG/cluster/*/*.parquet" \
    #     --columns_from_primary_parquets "*" \
    #     --secondary_parquets_path "gs://sangraha/pdfs_out/$SAVE_LANG/*/cleaned_docs/cleaned_docs/dataset" \
    #     --columns_from_secondary_parquets "uncleaned_text" \
    #     --identifier_cols "doc_id" \
    #     --join_type "inner" \
    #     --custom_join_run_mode "stage" \
    #     --joined_output_path "gs://sangraha/pdfs_dedup_analysis/$SAVE_LANG/clusters"

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
    #     CustomJoinStage \
    #     --primary_parquets_path "gs://sangraha/pdfs_dedup_analysis/$SAVE_LANG/clusters/*.parquet" \
    #     --columns_from_primary_parquets "*" \
    #     --secondary_parquets_path "gs://sangraha/pdfs_out/$SAVE_LANG/*/json2parquets/dataset" \
    #     --columns_from_secondary_parquets "page_no,identifier,pdf_name" \
    #     --identifier_cols "doc_id" \
    #     --join_type "inner" \
    #     --custom_join_run_mode "stage" \
    #     --joined_output_path "gs://sangraha/pdfs_dedup_analysis/$SAVE_LANG/clusters_paged"

done