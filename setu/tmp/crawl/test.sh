#!/bin/bash

# Define the two arrays
# languages=("bengali" "gujarati" "kannada" "konkani" "maithili" "malayalam" "marathi" "nepali" "oriya" "punjabi" "sanskrit" "tamil")
# parts=(3 3 4 2 2 4 4 3 2 2 3 4)

# languages=("urdu" "telugu" "hindi" "marathi" "tamil")
# parts=(3 4 6 4 4)

languages=("tamil")
parts=(4)


# languages=("assamese")
# parts=(3)

# Get the length of one of the arrays (assuming both have the same length)
length=${#languages[@]}

# Loop through the indices of the arrays
for ((i = 0; i < length; i++)); do
    LANG="${languages[i]}"
    PART="${parts[i]}"
    echo "$LANG $PART"

    CLUSTER_NAME="setu-debugging-50-highmem"
    REGION="asia-south1"
    GS_SETU_ROOT="gs://sangraha/setu"
    GS_INPUT_ROOT="gs://sangraha/crawls_raw_600"
    GS_OUTPUT_ROOT="gs://sangraha/parquets"
    SAVE_LANG=`echo "$LANG" | tr '[:upper:]' '[:lower:]'`
    echo "Running for $SAVE_LANG"
    SHARD="*"
    OUTPUT_SHARD=$PART

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --py-files="gs://sangraha/setu/dataproc/envs/setu.zip" \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="50g","spark.driver.memory"="8g","spark.executor.cores"="9","spark.executor.instances"="1","spark.submit.deployMode"="client","spark.sql.parquet.columnarReaderBatchSize"="128","spark.sql.parquet.enableVectorizedReader"="true","spark.sql.shuffle.partitions"="1024" \
        gs://sangraha/setu/setu/run.py \
        -- \
        --config $GS_SETU_ROOT/dataproc/configs/crawls/spark_${SAVE_LANG}_config.json \
        --mode crawl \
        --run_local False \
        ExtractTextStage \
        --te_parquets_path "$GS_INPUT_ROOT/$SAVE_LANG/" \
        --te_samples_per_partition 1500 \
        --te_verbose False \
        --te_run_mode data \
        --te_output_path "$GS_OUTPUT_ROOT/$SAVE_LANG/$OUTPUT_SHARD" 

done

