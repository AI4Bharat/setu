# Check if the required number of arguments are provided
if [ "$#" -ne 8 ]; then
    echo "Usage: $0 -d <directory> -p <docs_per_partition> -s <base_save_dir> -b <run_batch> "
    exit 1
fi

while getopts "d:p:s:b:" opt; do
  case $opt in
    d) directory="$OPTARG" ;;
    p) docs_per_partition="$OPTARG" ;;
    s) base_save_dir="$OPTARG" ;;
    b) run_batch="$OPTARG" ;;
    \?) echo "Invalid option -$OPTARG" >&2; exit 1 ;;
    :) echo "Option -$OPTARG requires an argument." >&2; exit 1 ;;
  esac
done

if [[ "$run_batch" == "true" ]]; then
    echo "run_batch is true."
elif [[ "$run_batch" == "false" ]]; then
    echo "run_batch is false."
else
    echo "Invalid run_batch value. Only true/false allowed."
    exit 2
fi

echo -e "\n"
echo "Environment Variables:"
echo "SETU_DIR=$SETU_DIR"
echo "SETU_TMP_DIR=$SETU_TMP_DIR"
echo -e "\n"
echo "Provided Arguments:"
echo "directory=$directory"
echo "docs_per_partition=$docs_per_partition"
echo "base_save_dir=$base_save_dir"
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

    echo "Processing $BATCH..."

    echo "Are you running at dataset level? $on_dataset"

    if [ $on_dataset == true ]; then
        local batch_name="dataset"
    else
        local batch_name=$(basename "$BATCH")
        local batch_name="${batch_name%.*}"
    fi

    # Check if it's a regular file (not a directory)
    if [[ -d "$BATCH" && $on_dataset == false ]]; then
        BATCH="$BATCH/*.parquet"
    fi

    echo "Base Save Directory: $base_save_dir & Batch Name: $batch_name"
    echo -e "\n"
    echo "Parquet path provided: $BATCH"

    spark-submit \
            --master "spark://SPK-DGX-O1:7077" \
            --driver-java-options "-Djava.io.tmpdir=$SETU_TMP_DIR" \
            --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=$SETU_TMP_DIR" \
            --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=$SETU_TMP_DIR" \
            --conf "spark.worker.dir=$SETU_TMP_DIR" \
            --conf "spark.local.dir=$SETU_TMP_DIR" \
            --num-executors 96 \
            --executor-cores 1 \
            --executor-memory 4G \
            --driver-memory 10G \
            --py-files "$SETU_DIR/setu/constants.py,$SETU_DIR/setu/document_filters.py,$SETU_DIR/setu/line_filters.py,$SETU_DIR/setu/lid.py,$SETU_DIR/setu/utils.py,$SETU_DIR/setu/setu.py" \
            "$PYTHON_SCRIPT" \
            --config "$SETU_DIR/setu/configs/spark_config.json" \
            --parquet_glob_path "$BATCH" \
            --samples_per_partition $docs_per_partition \
            --save_doc_lid_output \
            --doc_lid_output_path "$base_save_dir/doc_lid/$batch_name" \
            --save_line_stats_output \
            --line_stats_output_path "$base_save_dir/line_stats/$batch_name" \
            --save_doc_stats_output \
            --doc_stats_output_path "$base_save_dir/doc_stats/$batch_name" \
            --save_nsfw_data \
            --nsfw_output_path "$base_save_dir/nsfw_docs/$batch_name" \
            --final_output_path "$base_save_dir/final_docs/$batch_name" >> "$base_save_dir/logs/$batch_name.txt"

    # Check the exit status of the Python script
    if [ $? -ne 0 ]; then
        echo "Python script encountered an error with $BATCH."
        # If you want to stop processing when an error occurs, uncomment the line below
        exit 3
    fi
}


if [[ "$directory" == *\** ]]; then

    echo "Proceeding for dataset spark-job..............."
    echo -e "\n"

    run_spark "$directory" true

elif [ -f "$directory" ]; then

    echo "Proceeding for single file spark-job..............."
    echo -e "\n"

    run_spark $directory false

elif [[ -d "$directory" && $run_batch == "true" ]]; then

    echo "Proceeding for single batch spark-job..............."
    echo -e "\n"

    run_spark $directory false
    
elif [[ -d "$directory" && $run_batch == "false" ]]; then

    echo "Proceeding for multi-batch spark-job..............."
    echo -e "\n"

    TOTAL_BATCHS=$(find "$directory" -maxdepth 1 -mindepth 1 | wc -l)

    count=0

    # Loop over each file in the directory and run the Python script on it
    for BATCH in "$directory"/*; do

        print_progress $count $TOTAL_BATCHS

        run_spark $BATCH false

        ((count++))

    done

else

    echo "Error: Path $directory is neither a file nor a directory."
    exit 2

fi

