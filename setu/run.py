import argparse
from setu import Setu
from pyspark.sql import SparkSession
import threading
import traceback
from parse_args import parse_args

if __name__ == "__main__":

    args = parse_args()

    print("Entered Commandline Arguments: ", args)

    setu = Setu(config_file=args.config)

    spark = SparkSession \
                .builder \
                .appName(setu.config.appname) \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.sql.broadcastTimeout", "36000") \
                .config("spark.driver.maxResultSize", "0") \
                .config('spark.sql.autoBroadcastJoinThreshold', '-1') \
                .config('spark.sql.adaptive.enabled', 'true') \
                .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
                .config('spark.speculation', 'true') \
                .getOrCreate()

    spark_context = spark.sparkContext

    spark.sparkContext.setCheckpointDir(args.checkpoint_dir)
    
    try:

        if args.run_doc_clean:

            if not args.cleaned_doc_output_path and ( args.save_symbol_heavy_docs and not args.symbol_filter_output_path ):
                raise Exception(f"Please provide `cleaned_doc_output_path` an/or if `symbol_filter_output_path` if `save_symbol_heavy_docs` is enabled.....")

            if args.is_doc_df_path_batched:
                with open(args.doc_df_parquets_path) as batch_f:
                    parquet_list = [line.strip() for line in batch_f.readlines()]
                doc_df = spark.read.format("parquet").options(header='true', inferschema='true').load(parquet_list)
            else:
                doc_df = spark.read.format("parquet").options(header='true', inferschema='true').load(args.doc_df_parquets_path)

            setu.run_doc_clean_spark_pipeline(
                spark,
                doc_df,
                cols_to_use=[
                    "doc_id", "url", "source", "text", 
                    "language", "successful_extraction"
                ],
                doc_id_col="doc_id",
                text_col="text",
                docs_per_partition=args.samples_per_partition,
                use_symbol_filter=args.use_symbol_filter,
                save_symbol_heavy_docs=args.save_symbol_heavy_docs,
                symbol_filter_output_path=args.symbol_filter_output_path,
                cleaned_doc_output_path=args.cleaned_doc_output_path,
                run_data_parallel_mode=args.run_data_parallel_mode,
                verbose=args.verbose,
            )

        if args.run_lid_segregation:

            if not args.doc_lid_output_path and ( args.save_symbol_heavy_docs and not args.symbol_filter_output_path ):
                raise Exception(f"Please provide `doc_lid_output_path` an/or if `symbol_filter_output_path` if `save_symbol_heavy_docs` is enabled.....")

            if args.is_lid_df_path_batched:
                with open(args.lid_df_parquets_path) as batch_f:
                    parquet_list = [line.strip() for line in batch_f.readlines()]
                lid_df = spark.read.format("parquet").options(header='true', inferschema='true').load(parquet_list)
            else:
                lid_df = spark.read.format("parquet").options(header='true', inferschema='true').load(args.lid_df_parquets_path)

            setu.run_lid_segregation_spark_pipeline(
                spark,
                lid_df,
                doc_id_col="doc_id",
                text_col="text",
                docs_per_partition=args.samples_per_partition,
                doc_lid_output_path=args.doc_lid_output_path,
                verbose=args.verbose,
            )

        if args.run_analysis:

            if not args.line_stats_output_path or not args.doc_stats_output_path or not args.analysis_output_path:
                raise Exception(f"Please provide whichever one is left: `line_stats_output_path`, `doc_stats_output_path`, `analysis_output_path`.....")

            if args.is_analysis_df_path_batched:
                with open(args.analysis_df_parquets_path) as batch_f:
                    parquet_list = [line.strip() for line in batch_f.readlines()]
                analysis_df = spark.read.format("parquet").options(header='true', inferschema='true').load(parquet_list)
            else:
                analysis_df = spark.read.format("parquet").options(header='true', inferschema='true').load(args.analysis_df_parquets_path)

            setu.run_analysis_spark_pipeline(
                analysis_df,
                # cols_to_use=[
                #     "doc_id", "url", "source", "text", 
                #     "language", "doc_lang", "doc_lang_iso",
                #     "uncleaned_chars_count", "uncleaned_words_count", 
                #     "uncleaned_bytes", "symbol_ratio", "invalid_char_count"
                # ],
                cols_to_use=[
                    "doc_id", "text", "doc_lang", "doc_lang_iso"
                ],
                doc_id_col="doc_id",
                text_col="text",
                docs_per_partition=args.samples_per_partition,
                line_stats_output_path=args.line_stats_output_path,
                doc_stats_output_path=args.doc_stats_output_path,
                analysis_output_path=args.analysis_output_path,
                verbose=args.verbose,
            )

        if args.run_flag_and_filter:

            if not args.filtered_doc_stats_output_path or (args.save_nsfw_data and not args.nsfw_output_path):
                raise Exception(f"Please provide `filtered_doc_stats_output_path` and/or `nsfw_output_path` if `save_nsfw_data` is enabled.....")

            if args.is_doc_stats_path_batched:
                with open(args.doc_stats_parquets_path) as batch_f:
                    parquet_list = [line.strip() for line in batch_f.readlines()]
                doc_stats_df = spark.read.format("parquet").options(header='true', inferschema='true').load(parquet_list)
            else:
                doc_stats_df = spark.read.format("parquet").options(header='true', inferschema='true').load(args.doc_stats_parquets_path)

            setu.run_flagging_and_filtering_spark_pipeline(
                doc_stats_df,
                docs_per_partition=args.samples_per_partition,
                save_nsfw_data=args.save_nsfw_data,
                nsfw_output_path=args.nsfw_output_path,
                filtered_doc_stats_output_path=args.filtered_doc_stats_output_path,
                verbose=args.verbose,
            )

        if args.run_document_removal:

            if not args.filtered_docs_path:
                raise Exception(f"Please provide `filtered_docs_path`..... current value: {args.filtered_docs_path}")

            if args.is_doc_stats_path_batched:
                raise Exception("For document removal stage - `is_doc_stats_path_batched` cannot be set to true.")

            if args.is_analysis_df_path_batched:
                with open(args.analysis_df_parquets_path) as batch_f:
                    parquet_list = [line.strip() for line in batch_f.readlines()]
                df = spark.read.format("parquet").options(header='true', inferschema='true').load(parquet_list)
            else:
                df = spark.read.format("parquet").options(header='true', inferschema='true').load(args.analysis_df_parquets_path)

            doc_stats_df = spark.read.format("parquet").options(header='true', inferschema='true').load(args.doc_stats_path_for_removal)

            setu.remove_documents(
                df=df,
                doc_stats_df=doc_stats_df,
                docs_per_partition=args.samples_per_partition,
                doc_id_col="doc_id",
                filtered_docs_path=args.filtered_docs_path,
            )

        if not spark_context._jsc.sc().isStopped():
                spark.stop()

    except Exception as e:

        print("Encountered an Error: ", e)
        traceback.print_exc()
        if not spark_context._jsc.sc().isStopped():
            spark.stop()
        raise Exception("Job Failed with above mentioned exception")
        

