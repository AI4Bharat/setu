#!/bin/bash

# Check if the required number of arguments are provided
# if [ "$#" -ne 26 ]; then
#     echo "Usage: $0 -i <language> -m <batch_info> -d <directories> -p <docs_per_partition> -s <base_save_dir> -b <run_batch> -c <checkpoint_dir> -l <run_lid_segregation> -a <run_analysis> -f <run_flag_and_filter> -t <save_nsfw_data> -r <run_document_removal> -q <doc_stats_path_for_removal> -v <verbose> -n <num_of_executors> -o <executor_cores> -e <executor_memory> -k <driver_memory> -x <spark_parallelism>"
#     exit 1
# fi

# default spark configs
num_of_executors="12"
executor_cores="8"
executor_memory="50G"
driver_memory="50G"
spark_parallelism="96"

while getopts "i:m:d:p:s:b:c:w:u:l:a:f:t:r:q:v:n:o:e:k:x:" opt; do
  case $opt in
    i) language="$OPTARG" ;;
    m) batch_info="$OPTARG" ;;
    d) directory="$OPTARG" ;;
    p) docs_per_partition="$OPTARG" ;;
    s) base_save_dir="$OPTARG" ;;
    b) run_batch="$OPTARG" ;;
    c) checkpoint_dir="$OPTARG" ;;
    w) run_data_parallel_mode="$OPTARG" ;;
    u) run_doc_clean="$OPTARG" ;;
    l) run_lid_segregation="$OPTARG" ;;
    a) run_analysis="$OPTARG" ;;
    f) run_flag_and_filter="$OPTARG" ;;
    t) save_nsfw_data="$OPTARG" ;;
    r) run_document_removal="$OPTARG" ;;
    q) doc_stats_path_for_removal="$OPTARG" ;;
    v) verbose="$OPTARG" ;;
    n) num_of_executors="$OPTARG" ;;
    o) executor_cores="$OPTARG" ;;
    e) executor_memory="$OPTARG" ;;
    k) driver_memory="$OPTARG" ;;
    x) spark_parallelism="$OPTARG" ;;
    \?) echo "Invalid option -$OPTARG" >&2; exit 1 ;;
    :) echo "Option -$OPTARG requires an argument." >&2; exit 1 ;;
  esac
done

check_boolean() {
    local arg=$1
    local arg_name=$2

    # Check if the argument is either 'true' or 'false'
    if [ "$arg" == "true" ] || [ "$arg" == "false" ]; then
        echo "Valid boolean argument: $arg_name"
    else
        echo "Invalid argument $arg_name. Please provide 'true' or 'false'."
        exit 1  # Exit with an error status
    fi
}

check_boolean $run_batch "run_batch"
check_boolean $verbose "verbose"
check_boolean $run_data_parallel_mode "run_data_parallel_mode"
check_boolean $run_doc_clean "run_doc_clean"
check_boolean $run_lid_segregation "run_lid_segregation"
check_boolean $run_analysis "run_analysis"
check_boolean $run_flag_and_filter "run_flag_and_filter"
check_boolean $run_document_removal "run_document_removal"
check_boolean $save_nsfw_data "save_nsfw_data"

echo -e "\n"
echo "Environment Variables:"
echo "SETU_DIR=$SETU_DIR" ## 
echo "SETU_TMP_DIR=$SETU_TMP_DIR"
echo -e "\n"
echo "Provided Arguments:"
echo "language=$language"
echo "batch_info=$batch_info"
echo "directory=$directory"
echo "docs_per_partition=$docs_per_partition"
echo "base_save_dir=$base_save_dir"
echo "checkpoint_dir=$checkpoint_dir"
echo "run_data_parallel_mode=$run_data_parallel_mode"
echo "run_doc_clean=$run_doc_clean"
echo "run_lid_segregation=$run_lid_segregation"
echo "run_analysis=$run_analysis"
echo "run_flag_and_filter=$run_flag_and_filter"
echo "save_nsfw_data=$save_nsfw_data"
echo "run_document_removal=$run_document_removal"
echo "doc_stats_path_for_removal=$doc_stats_path_for_removal"
echo "verbose=$verbose"
echo "num_of_executors=$num_of_executors"
echo "executor_cores=$executor_cores"
echo "executor_memory=$executor_memory"
echo "driver_memory=$driver_memory"
echo "spark_parallelism=$spark_parallelism"
echo -e "\n"

# Function to display the progress bar
print_progress() {
    local progress=$1
    local total=$2
    local barLength=50

    local filledLength=$(( $barLength * $progress / $total ))
    local progressBar=$(printf '#'%.0s $(seq 1 $filledLength))
    local emptySpaces=$(printf ' ' %.0s $(seq 1 $(( $barLength - $filledLength ))))

    # Save current cursor position
    tput sc

    # Move the cursor to the bottom of the terminal
    tput cup $(($(tput lines) - 1)) 0

    # Print the progress bar and percentage
    printf "Progress: |%s%s| %s%%" "$progressBar" "$emptySpaces" $(($progress*100/$total))

    # # Move the cursor down 3 lines for some space
    # tput cup 3 0

    # Restore cursor position
    tput rc
}

PYTHON_SCRIPT="$SETU_DIR/setu/run.py"

# Check if directory exists
if [ ! -d "$base_save_dir/logs" ]; then
    # Create directory including any necessary subdirectories
    mkdir -p "$base_save_dir/logs"
    echo "Created log directory at: $base_save_dir/logs"
else
    echo "log directory already present at: $base_save_dir/logs"
fi

echo -e "\n"


run_spark() {

    local BATCH=$1
    local on_dataset=$2
    local batch_name=$3
    local on_batch_info=$4
    local is_doc_df_path_batched=false
    local is_lid_df_path_batched=false
    local is_analysis_df_path_batched=false
    local is_doc_stats_path_batched=false

    if [ $on_batch_info == true ]; then
        echo "Processing line $batch_name in the provided batch_info: $batch_info..."
        local is_doc_df_path_batched=true
        local is_lid_df_path_batched=true
        local is_analysis_df_path_batched=true
        local is_doc_stats_path_batched=true
    else
        echo "Processing $BATCH..."
    fi

    echo "Are you running at dataset level? $on_dataset"

    # Check if it's a regular file (not a directory)
    if [[ -d "$BATCH" && $on_dataset == false ]]; then
        BATCH="$BATCH/*.parquet"
    fi

    echo "Base Save Directory: $base_save_dir & Batch Name: $batch_name"
    echo -e "\n"
    if [ $on_batch_info == false ]; then
        echo "Parquet path provided: $BATCH"
    fi

    config_file="$SETU_DIR/setu/configs/spark_"$language"_config.json"
    echo "Config File: $config_file"

    spark-submit \
            --master "spark://SPK-DGX-O1:7077" \
            --driver-java-options "-Djava.io.tmpdir=$SETU_TMP_DIR" \
            --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=$SETU_TMP_DIR" \
            --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=$SETU_TMP_DIR" \
            --conf "spark.worker.dir=$SETU_TMP_DIR" \
            --conf "spark.local.dir=$SETU_TMP_DIR" \
            --conf "spark.sql.autoBroadcastJoinThreshold=1073741824" \
            --conf "spark.default.parallelism=$spark_parallelism" \
            --conf "spark.sql.shuffle.partitions=512" \
            --num-executors $num_of_executors \
            --executor-cores $executor_cores \
            --executor-memory $executor_memory \
            --driver-memory $driver_memory \
            --py-files "$SETU_DIR/setu/parse_args.py,$SETU_DIR/setu/constants.py,$SETU_DIR/setu/document_filters.py,$SETU_DIR/setu/line_filters.py,$SETU_DIR/setu/lid.py,$SETU_DIR/setu/utils.py,$SETU_DIR/setu/setu.py" \
            "$PYTHON_SCRIPT" \
            --config "$config_file" \
            --samples_per_partition $docs_per_partition \
            --verbose $verbose \
            --checkpoint_dir "$checkpoint_dir" \
            --run_data_parallel_mode "$run_data_parallel_mode" \
            --run_doc_clean "$run_doc_clean" \
            --doc_df_parquets_path "$BATCH" \
            --is_doc_df_path_batched "$is_doc_df_   path_batched" \
            --use_symbol_filter "True" \
            --save_symbol_heavy_docs "True" \
            --symbol_filter_output_path "$base_save_dir/symbol_heavy/$batch_name" \
            --cleaned_doc_output_path "$base_save_dir/cleaned_docs/$batch_name" \
            --run_lid_segregation $run_lid_segregation \
            --lid_df_parquets_path "$BATCH" \
            --is_lid_df_path_batched "$is_lid_df_path_batched" \
            --doc_lid_output_path "$base_save_dir/doc_lid/$batch_name" \
            --run_analysis $run_analysis \
            --analysis_df_parquets_path "$BATCH" \
            --is_analysis_df_path_batched "$is_analysis_df_path_batched" \
            --line_stats_output_path "$base_save_dir/line_stats/$batch_name" \
            --doc_stats_output_path "$base_save_dir/doc_stats/$batch_name" \
            --analysis_output_path "$base_save_dir/analysis/$batch_name" \
            --run_flag_and_filter $run_flag_and_filter \
            --doc_stats_parquets_path "$BATCH" \
            --is_doc_stats_path_batched "$is_doc_stats_path_batched" \
            --save_nsfw_data $save_nsfw_data \
            --nsfw_output_path "$base_save_dir/nsfw_doc_stats/$batch_name" \
            --filtered_doc_stats_output_path "$base_save_dir/filtered_doc_stats/$batch_name" \
            --run_document_removal $run_document_removal \
            --doc_stats_path_for_removal "$doc_stats_path_for_removal" \
            --filtered_docs_path "$base_save_dir/filtered_docs/$batch_name" \
             >> "$base_save_dir/logs/$batch_name.txt"

    # Check the exit status of the Python script
    if [ $? -ne 0 ]; then
        echo "Python script encountered an error with $BATCH."
        # If you want to stop processing when an error occurs, uncomment the line below
        exit 3
    fi
}

if [ "$batch_info" ]; then

    echo "If you want to run at directory level - please don't provide: batch_info"

    batch_no=0

    for line in $(cat "$batch_info")
    do

        run_spark $line false $batch_no true
        ((batch_no++))
 
    done

elif [[ "$directory" == *\** ]]; then

    echo "Proceeding for dataset spark-job..............."
    echo -e "\n"

    run_spark "$directory" true "dataset" false

elif [ -f "$directory" ]; then

    echo "Proceeding for single file spark-job..............."
    echo -e "\n"

    batch_name=$(basename "$BATCH")
    batch_name="${batch_name%.*}"

    run_spark $directory false $batch_name false

elif [[ -d "$directory" && $run_batch == "true" ]]; then

    echo "Proceeding for single batch spark-job..............."
    echo -e "\n"

    batch_name=$(basename "$BATCH")
    batch_name="${batch_name%.*}"

    run_spark $directory false $batch_name false
    
elif [[ -d "$directory" && $run_batch == "false" ]]; then

    echo "Proceeding for multi-batch spark-job..............."
    echo -e "\n"

    TOTAL_BATCHS=$(find "$directory" -maxdepth 1 -mindepth 1 | wc -l)

    count=0

    # Loop over each file in the directory and run the Python script on it
    for BATCH in "$directory"/*; do

        print_progress $count $TOTAL_BATCHS

        batch_name=$(basename "$BATCH")
        batch_name="${batch_name%.*}"

        run_spark $BATCH false $batch_name false

        ((count++))

    done

else

    echo "Error: Path $directory is neither a file nor a directory."
    exit 2

fi

