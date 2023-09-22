import re
from pyspark.sql.functions import (
    udf, 
    split, 
    explode, 
    concat_ws,
    count,
    sum,
    avg,
    # median,   # Not using temporarily as GCP's spark version is 3.3 and this was released in 3.4 
    # mode,     # Not using temporarily as GCP's spark version is 3.3 and this was released in 3.4
    min,
    max,
    when,
    col,
    create_map,
    collect_list,
    expr,
    map_from_arrays,
    posexplode,
    struct,
    broadcast,
    rand
)

from pyspark.sql.types import (
    BooleanType,
    IntegerType, 
    StringType, 
)
from functools import partial
import json
from pyspark.sql.window import Window
import os
import shutil

def rename_partitioned_directories(base_dir, partition_column_name):
    for dir_name in os.listdir(base_dir):
        if dir_name.startswith(partition_column_name + "="):
            new_name = dir_name.split("=")[1]
            old_path = os.path.join(base_dir, dir_name)
            new_path = os.path.join(base_dir, new_name)
            shutil.move(old_path, new_path)

class ChunkHandler():

    def doc2lines(self, df, text_column, split_symbol):

        df = df \
            .withColumn(text_column, split(text_column, split_symbol, -1)) \
            .select("*", posexplode(text_column)).drop(text_column).withColumnRenamed("col", text_column) \
        
        return df

    def lines2doc(self, df, text_column, identifier_column, sort_column, join_symbol):

        join_lines = udf(lambda x: join_symbol.join([line.text for line in x if line]), StringType())

        df = df.withColumn(text_column, struct([sort_column, text_column])).select(identifier_column, text_column) \
                .groupBy(identifier_column) \
                .agg(collect_list(text_column).alias(text_column)) \
                .withColumn(
                    text_column,
                    expr(
                        f"array_sort(transform({text_column},x->struct(x['{sort_column}'] as {sort_column},x['{text_column}'] as {text_column})))"
                    )   
                ) \
                .withColumn(text_column, join_lines(text_column))

        return df
    
class SparkOptimizedHandlers():

    def get_num_lines(self, grouped_line_df):
        lines_count = grouped_line_df.agg(count("*").alias("lines_count"))
        return lines_count

    def get_mean_line_length(self, grouped_line_df, line_len_col_):
        mean_line_lengths = grouped_line_df.agg(avg(line_len_col_).alias("mean_line_length"))
        return mean_line_lengths

    def get_min_line_length(self, grouped_line_df, line_len_col_):
        min_line_lengths_col = grouped_line_df.agg(min(line_len_col_).alias("min_line_length"))
        return min_line_lengths_col

    def get_max_line_length(self, grouped_line_df, line_len_col_):
        max_line_lengths = grouped_line_df.agg(max(line_len_col_).alias("max_line_length"))
        return max_line_lengths

    def get_nsfw_words_count(self, grouped_line_df, line_nsfw_count_col_):
        nsfw_count = grouped_line_df.agg(sum(line_nsfw_count_col_).alias("nsfw_words_count"))
        return nsfw_count

    def get_non_li_words_count(self, grouped_line_df, line_non_li_count_col_):
        non_li_count = grouped_line_df.agg(sum(line_non_li_count_col_).alias("non_li_char_count"))
        return non_li_count

    def get_bytes(self, grouped_line_df, line_bytes_col_):
        bytes_ = grouped_line_df.agg(sum(line_bytes_col_).alias("bytes"))
        return bytes_

    def get_words_count(self, grouped_line_df, line_words_count_col_):
        word_count = grouped_line_df.agg(sum(line_words_count_col_).alias("words_count"))
        return word_count

    def get_char_count(self, grouped_line_df, line_char_count_col_):
        char_count = grouped_line_df.agg(sum(line_char_count_col_).alias("char_count"))
        return char_count

    def get_repeated_line_dist(self, line_df, id_col, text_col):

        col_name = "repeated_line_dist"

        repeated_line_dist = line_df.groupBy(id_col, text_col) \
                                    .agg(count("*").alias(col_name)) \
                                    .groupBy(id_col) \
                                    .agg(collect_list(create_map([text_col, col_name])).alias(col_name)) \
                                    .withColumn("keys", expr(f"transform({col_name}, x -> map_keys(x)[0])")) \
                                    .withColumn("values", expr(f"transform({col_name}, x -> map_values(x)[0])")) \
                                    .withColumn(col_name, map_from_arrays(col("keys"), col("values"))) \
                                    .drop("keys", "values")

        return repeated_line_dist

    def run_analysis(
        self,
        line_df,
        doc_id_col,
        text_col,
        line_nsfw_count_col_,
        line_non_li_count_col_,
        line_bytes_col_,
        line_words_count_col_,
        line_char_count_col_,
    ):
        
        grouped_line_df = line_df.groupBy(doc_id_col)

        num_lines_df = self.get_num_lines(grouped_line_df)
        nsfw_words_count_df = self.get_nsfw_words_count(grouped_line_df, line_nsfw_count_col_)
        non_li_words_count_df = self.get_non_li_words_count(grouped_line_df, line_non_li_count_col_)
        bytes_df = self.get_bytes(grouped_line_df, line_bytes_col_)
        words_count_df = self.get_words_count(grouped_line_df, line_words_count_col_)
        char_count_df = self.get_char_count(grouped_line_df, line_char_count_col_)
        mean_line_len_df = self.get_mean_line_length(grouped_line_df, "words_count")
        min_line_len_df = self.get_min_line_length(grouped_line_df, "words_count")
        max_line_len_df = self.get_max_line_length(grouped_line_df, "words_count")

        doc_df = num_lines_df \
                .join(mean_line_len_df, [doc_id_col]) \
                .join(min_line_len_df, [doc_id_col]) \
                .join(max_line_len_df, [doc_id_col]) \
                .join(nsfw_words_count_df, [doc_id_col]) \
                .join(non_li_words_count_df, [doc_id_col]) \
                .join(bytes_df, [doc_id_col]) \
                .join(words_count_df, [doc_id_col]) \
                .join(char_count_df, [doc_id_col])
        
        return doc_df

    def run_flagging(
        self, 
        doc_df,
        word_count_col,
        char_count_col,
        nsfw_count_col,
        nsfw_threshold,
        non_li_count_col, 
        non_li_threshold,
        min_line_count,
        line_count_col,
        min_mean_line_len,
        mean_line_len_col,
    ):
        doc_df = doc_df \
                .select("*", when(doc_df[line_count_col] <= min_line_count, True).otherwise(False).alias("has_less_lines")) \
                .select("*", when(doc_df[mean_line_len_col] <= min_mean_line_len, True).otherwise(False).alias("is_short_lines_heavy")) \
                .select("*", when(doc_df[nsfw_count_col]/doc_df[word_count_col] >= nsfw_threshold, True).otherwise(False).alias("is_nsfw_heavy")) \
                .select("*", when(doc_df[non_li_count_col]/doc_df[char_count_col] >= non_li_threshold, True).otherwise(False).alias("is_non_li_heavy"))
        
        return doc_df
