# Setu Commands 

Before running the various stages and the pipeline ensure that you have started the Spark Master session and a worker session.

```bash
$SPARK_HOME/sbin/start-master.sh
```

Once you start the master session you can copy the session URL by viewing the details at ```http://localhost:8080/```. Use the URL and start a worker session.

```bash
$SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER_URL
```

# Stage Wise Commands

Before running the commands make see to that you are in the setu/setu directory and ensure that all data paths and arguments are set accordingly.

# JSON2ParquetStage

```bash
SETU_DIR=/home/$USER/setu SETU_TMP_DIR=/home/$USER/tmp/ FILTER_DATA_ROOT=/home/$USER/setu/setu/data \
    spark-submit \
    --master $SPARK_MASTER_URL \
    --deploy-mode client \
    --driver-java-options -Djava.io.tmpdir=/home/$USER/tmp/ \
    --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=/home/$USER/tmp/" \
    --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=/home/$USER/tmp/" \
    --conf spark.worker.dir="/home/$USER/tmp/" \
    --conf spark.local.dir="/home/$USER/tmp/" \
    --num-executors 4 \
    --executor-cores 2 \
    --executor-memory 3G \
    --driver-memory 6G \
    --archives "/home/$USER/setu/dataproc/envs/setu.zip" \
    --conf 'spark.executorEnv.PYTHONPATH=setu.zip' \
    --conf 'spark.executorEnv.FILTER_DATA_ROOT=setu.zip/data' \
    run.py \
    --config /home/$USER/setu/configs/crawls/spark_english_config.json \
    --mode crawl \
    --run_local True \
    JSON2ParquetStage \
    --json_glob_path "/home/$USER/setu/examples/sample_data/*.json" \
    --language english \
    --j2p_samples_per_partition 1500 \
    --j2p_verbose False \
    --j2p_run_mode data \
    --j2p_parquet_output_path /home/$USER/setu/examples/output/j2p_output
```

# TextExtractStage

```bash
!SETU_DIR=/home/$USER/setu SETU_TMP_DIR=/home/$USER/tmp/ FILTER_DATA_ROOT=/home/$USER/setu/setu/data \
    spark-submit \
    --master $SPARK_MASTER_URL \
    --deploy-mode client \
    --driver-java-options -Djava.io.tmpdir=/home/$USER/tmp/ \
    --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=/home/$USER/tmp/" \
    --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=/home/$USER/tmp/" \
    --conf spark.worker.dir="/home/$USER/tmp/" \
    --conf spark.local.dir="/home/$USER/tmp/" \
    --num-executors 4 \
    --executor-cores 2 \
    --executor-memory 3G \
    --driver-memory 6G \
    --archives "/home/$USER/setu/dataproc/envs/setu.zip" \
    --conf 'spark.executorEnv.PYTHONPATH=setu.zip' \
    --conf 'spark.executorEnv.FILTER_DATA_ROOT=setu.zip/data' \
    /home/$USER/setu/setu/run.py \
    --config /home/$USER/setu/configs/crawls/spark_english_config.json \
    --mode crawl \
    --run_local True \
    ExtractTextStage \
    --te_parquets_path "/home/$USER/setu/examples/output/j2p_output/*.parquet" \
    --te_samples_per_partition 1500 \
    --te_run_mode data \
    --te_output_path "/home/$USER/setu/examples/output/te_output"

```

# DocCleanStage

```bash
SETU_DIR=/home/$USER/setu SETU_TMP_DIR=/home/$USER/tmp/ FILTER_DATA_ROOT=/home/$USER/setu/setu/data \
    spark-submit \
    --master $SPARK_MASTER_URL \
    --deploy-mode client \
    --driver-java-options -Djava.io.tmpdir=/home/$USER/tmp/ \
    --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=/home/$USER/tmp/" \
    --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=/home/$USER/tmp/" \
    --conf spark.worker.dir="/home/$USER/tmp/" \
    --conf spark.local.dir="/home/$USER/tmp/" \
    --num-executors 4 \
    --executor-cores 2 \
    --executor-memory 3G \
    --driver-memory 6G \
    --archives "/home/$USER/setu/dataproc/envs/setu.zip" \
    --conf 'spark.executorEnv.PYTHONPATH=setu.zip' \
    --conf 'spark.executorEnv.FILTER_DATA_ROOT=setu.zip/data' \
    run.py \
    --config /home/$USER/setu/configs/crawls/spark_english_config.json \
    --mode crawl \
    --run_local True \
    DocCleanStage \
    --doc_df_parquets_path "/home/$USER/sample_data/output/j2p_output/*.parquet" \
    --is_doc_df_path_batched False \
    --doc_clean_additional_cols_to_use "url,source,language" \
    --use_symbol_filter True \
    --doc_clean_samples_per_partition 1500 \
    --doc_clean_verbose False \
    --doc_clean_run_mode data \
    --save_symbol_heavy_docs True \
    --symbol_filter_output_path "/home/$USER/sample_data/symbol_filter/" \
    --cleaned_doc_output_path "/home/$USER/sample_data/cleaned_docs/"
```


# LIDStage

```bash
SETU_DIR=/home/$USER/setu SETU_TMP_DIR=/home/$USER/tmp/ FILTER_DATA_ROOT=/home/$USER/setu/setu/data \
    spark-submit \
    --master $SPARK_MASTER_URL \
    --deploy-mode client \
    --driver-java-options -Djava.io.tmpdir=/home/$USER/tmp/ \
    --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=/home/$USER/tmp/" \
    --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=/home/$USER/tmp/" \
    --conf spark.worker.dir="/home/$USER/tmp/" \
    --conf spark.local.dir="/home/$USER/tmp/" \
    --conf spark.sql.execution.arrow.pyspark.enabled=true \
    --conf spark.sql.broadcastTimeout=36000 \
    --conf spark.driver.maxResultSize=0 \
    --conf spark.sql.autoBroadcastJoinThreshold=-1 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.serializer='org.apache.spark.serializer.KryoSerializer' \
    --conf spark.speculation=true \
    --conf "spark.default.parallelism=128" \
    --conf "spark.sql.shuffle.partitions=512" \
    --num-executors 16 \
    --executor-cores 8 \
    --executor-memory 32G \
    --driver-memory 50G \
    --archives "/home/$USER/setu/dataproc/envs/setu.zip" \
    --conf 'spark.executorEnv.PYTHONPATH=setu.zip' \
    --conf 'spark.executorEnv.FILTER_DATA_ROOT=setu.zip/data' \
    run.py \
    --config /home/$USER/setu/configs/crawls/spark_english_config.json \
    --mode crawl \
    --run_local True \
    LIDStage \
    --lid_df_parquets_path "/home/$USER/sample_data/cleaned_docs/*.parquet" \
    --is_lid_df_path_batched False \
    --lid_additional_cols "url,source,language" \
    --lid_samples_per_partition 1500 \
    --lid_verbose False \
    --lid_run_mode data \
    --doc_lid_output_path "/home/$USER/sample_data/lid/"
```


# AnalysisStage

```bash
SETU_DIR=/home/$USER/setu SETU_TMP_DIR=/home/$USER/tmp/ FILTER_DATA_ROOT=/home/$USER/setu/setu/data \
    spark-submit \
    --master $SPARK_MASTER_URL \
    --deploy-mode client \
    --driver-java-options -Djava.io.tmpdir=/home/$USER/tmp/ \
    --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=/home/$USER/tmp/" \
    --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=/home/$USER/tmp/" \
    --conf spark.worker.dir="/home/$USER/tmp/" \
    --conf spark.local.dir="/home/$USER/tmp/" \
    --conf spark.sql.execution.arrow.pyspark.enabled=true \
    --conf spark.sql.broadcastTimeout=36000 \
    --conf spark.driver.maxResultSize=0 \
    --conf spark.sql.autoBroadcastJoinThreshold=-1 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.serializer='org.apache.spark.serializer.KryoSerializer' \
    --conf spark.speculation=true \
    --conf "spark.default.parallelism=128" \
    --conf "spark.sql.shuffle.partitions=512" \
    --num-executors 16 \
    --executor-cores 8 \
    --executor-memory 32G \
    --driver-memory 50G \
    --archives "/home/$USER/setu/dataproc/envs/setu.zip" \
    --conf 'spark.executorEnv.PYTHONPATH=setu.zip' \
    --conf 'spark.executorEnv.FILTER_DATA_ROOT=setu.zip/data' \
    run.py \
    --config /home/$USER/setu/configs/crawls/spark_english_config.json \
    --mode crawl \
    --run_local True \
    AnalysisStage \
    --analysis_df_parquets_path "/home/$USER/sample_data/lid/*/*.parquet" \
    --is_analysis_df_path_batched False \
    --analysis_additional_cols_to_use "url,source,language,doc_lang,doc_lang_iso" \
    --analysis_samples_per_partition 1500 \
    --analysis_verbose False \
    --analysis_run_mode stage \
    --line_stats_output_path "/home/$USER/sample_data/line_stats/" \
    --doc_stats_output_path "/home/$USER/sample_data/doc_stats/" \
    --analysis_output_path "/home/$USER/sample_data/analysis/"
```

# FlaggingAndFilteringStage

```bash
SETU_DIR=/home/$USER/setu SETU_TMP_DIR=/home/$USER/tmp/ FILTER_DATA_ROOT=/home/$USER/setu/setu/data \
    spark-submit \
    --master $SPARK_MASTER_URL \
    --deploy-mode client \
    --driver-java-options -Djava.io.tmpdir=/home/$USER/tmp/ \
    --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=/home/$USER/tmp/" \
    --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=/home/$USER/tmp/" \
    --conf spark.worker.dir="/home/$USER/tmp/" \
    --conf spark.local.dir="/home/$USER/tmp/" \
    --conf spark.sql.execution.arrow.pyspark.enabled=true \
    --conf spark.sql.broadcastTimeout=36000 \
    --conf spark.driver.maxResultSize=0 \
    --conf spark.sql.autoBroadcastJoinThreshold=-1 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.serializer='org.apache.spark.serializer.KryoSerializer' \
    --conf spark.speculation=true \
    --conf "spark.default.parallelism=128" \
    --conf "spark.sql.shuffle.partitions=512" \
    --num-executors 16 \
    --executor-cores 8 \
    --executor-memory 32G \
    --driver-memory 50G \
    --archives "/home/$USER/setu/dataproc/envs/setu.zip" \
    --conf 'spark.executorEnv.PYTHONPATH=setu.zip' \
    --conf 'spark.executorEnv.FILTER_DATA_ROOT=setu.zip/data' \
    run.py \
    --config /home/$USER/setu/configs/crawls/spark_english_config.json \
    --mode crawl \
    --run_local True \
    FlaggingAndFilteringStage \
    --doc_stats_parquets_path "/home/$USER/sample_data/doc_stats/*/*.parquet" \
    --is_doc_stats_path_batched False \
    --fnf_samples_per_partition 1500 \
    --fnf_verbose False \
    --fnf_run_mode stage \
    --save_nsfw_data True \
    --nsfw_output_path "/home/$USER/sample_data/nsfw/" \
    --filtered_doc_stats_output_path "/home/$USER/sample_data/filtered_doc_stats/"
```

# DocumentRemovalStage

```bash
SETU_DIR=/home/$USER/setu SETU_TMP_DIR=/home/$USER/tmp/ FILTER_DATA_ROOT=/home/$USER/setu/setu/data \
    spark-submit \
    --master $SPARK_MASTER_URL \
    --deploy-mode client \
    --driver-java-options -Djava.io.tmpdir=/home/$USER/tmp/ \
    --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=/home/$USER/tmp/" \
    --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=/home/$USER/tmp/" \
    --conf spark.worker.dir="/home/$USER/tmp/" \
    --conf spark.local.dir="/home/$USER/tmp/" \
    --conf spark.sql.execution.arrow.pyspark.enabled=true \
    --conf spark.sql.broadcastTimeout=36000 \
    --conf spark.driver.maxResultSize=0 \
    --conf spark.sql.autoBroadcastJoinThreshold=-1 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.serializer='org.apache.spark.serializer.KryoSerializer' \
    --conf spark.speculation=true \
    --conf "spark.default.parallelism=128" \
    --conf "spark.sql.shuffle.partitions=512" \
    --num-executors 16 \
    --executor-cores 8 \
    --executor-memory 32G \
    --driver-memory 50G \
    --archives "/home/$USER/setu/dataproc/envs/setu.zip" \
    --conf 'spark.executorEnv.PYTHONPATH=setu.zip' \
    --conf 'spark.executorEnv.FILTER_DATA_ROOT=setu.zip/data' \
    run.py \
    --config /home/$USER/setu/configs/crawls/spark_english_config.json \
    --mode crawl \
    --run_local True \
    DocumentRemovalStage \
    --analysis_out_path "/home/$USER/sample_data/analysis/*/*.parquet" \
    --doc_stats_path "/home/$USER/sample_data/doc_stats/*/*.parquet" \
    --doc_removal_join_col "doc_id" \
    --doc_removal_samples_per_partition 1500 \
    --doc_removal_verbose False \
    --doc_removal_run_mode stage \
    --filtered_docs_path "/home/$USER/sample_data/filtered_doc_stats/"
```