

while getopts "l:m:d:p:s:b:c:a:f:o:r:v:" opt; do
  case $opt in
    l) language="$OPTARG" ;;
    m) batch_info="$OPTARG" ;;
    d) directory="$OPTARG" ;;
    p) docs_per_partition="$OPTARG" ;;
    s) base_save_dir="$OPTARG" ;;
    b) run_batch="$OPTARG" ;;
    c) checkpoint_dir="$OPTARG" ;;
    a) run_analysis="$OPTARG" ;;
    f) run_flag_and_filter="$OPTARG" ;;
    o) doc_stats_parquets_path="$OPTARG" ;;
    r) remove_documents="$OPTARG" ;;
    v) verbose="$OPTARG" ;;
    \?) echo "Invalid option -$OPTARG" >&2; exit 1 ;;
    :) echo "Option -$OPTARG requires an argument." >&2; exit 1 ;;
  esac
done

if [[ "$run_minhash" && "$local" ]]; then

    python -m text_dedup.minhash \
        --path "parquet" \
        --name "sangraha-hindi" \
        --split "train" \
        --data_files "/data/priyam/sangraha/spark_out/hindi/1/filtered_docs/filtered_docs/dataset/*.parquet" \
        --cache_dir "/data/priyam/cache" \
        --output "/data/priyam/sangraha/dedup/minhash/hindi/1" \
        --column "text" \
        --batch_size 10000

elif [[ "$run_minhash" && "$dataproc" ]]; then

    export CLUSTER_NAME=$cluster_name
    export PROJECT_ID=$project_id
    export REGION=$cluster_region
    export ZONE=$cluster_zone
    export INPUT_GCS_PATH=$input_gcs_path
    export OUTPUT_GCS_PATH=$output_gcs_path
    export MASTER_MACHINE_TYPE=$master_machine_type
    export MASTER_BOOT_DISK_SIZE=$master_boot_disk_size
    export WORKER_COUNT=$worker_count
    export WORKER_MACHINE_TYPE=$worker_machine_type
    export WORKER_BOOT_DISK_SIZE=$worker_boot_disk_size
    export IMAGE_VERSION=$image_version

    gcloud dataproc clusters create $CLUSTER_NAME \
        --enable-component-gateway \
        --region $REGION \
        --zone $ZONE \
        --master-machine-type $MASTER_MACHINE_TYPE \
        --master-boot-disk-size $MASTER_BOOT_DISK_SIZE \
        --num-workers $WORKER_COUNT \
        --worker-machine-type $WORKER_MACHINE_TYPE \
        --worker-boot-disk-size $WORKER_BOOT_DISK_SIZE \
        --image-version $IMAGE_VERSION \
        --project $PROJECT_ID

    gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
        --region $REGION \
        --jars gs://spark-lib/bigquery/spark-3.3-bigquery-0.32.2.jar \
        --driver-log-levels root=FATAL,__main__=DEBUG \
        --properties="spark.executor.memory"="$spark_executor_memory","spark.driver.memory"="$spark_driver_memory","spark.executor.cores"="$spark_executor_count" \
        $SETU_DIR/setu/text-dedup/text_dedup/minhash_spark.py \
        --input $INPUT_GCS_PATH \
        --output $OUTPUT_GCS_PATH

fi

if [ "$run_exact" ]; then
      python -m text_dedup.suffix_array \
        --path "arrow" \
        --name "sangraha-hindi" \
        --split "train" \
        --data_files "/data/priyam/sangraha/dedup/minhash/hindi/1/*.arrow" \
        --cache_dir "/data/priyam/cache" \
        --output "/data/priyam/sangraha/dedup/exact/hindi/1" \
        --column "text" \
        --google_repo_path "/data/priyam/setu/text-dedup/deduplicate-text-datasets"
fi