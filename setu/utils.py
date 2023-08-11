import re
from pyspark.sql.functions import (
    udf, 
    split, 
    explode, 
    concat_ws,
    count,
    sum,
    avg,
    median,
    mode,
    min,
    max,
    when,
    col,
    create_map,
    collect_list,
    expr,
    map_from_arrays,
    posexplode,
    struct
)
from pyspark.sql.types import (
    BooleanType,
    IntegerType, 
    StringType, 
)
from functools import partial
import json
from pyspark.sql.window import Window

class ChunkHandler():

    def doc2lines(self, df, text_column, split_symbol):
        df = df \
            .withColumn(text_column, split(text_column, split_symbol, -1)) \
            .select("*", posexplode(text_column)).drop(text_column).withColumnRenamed("col", text_column) \
        
        return df

    def lines2doc(self, df, text_column, identifier_column, sort_column, join_symbol):

        join_lines = udf(lambda x: join_symbol.join([line.text for line in x if line]), StringType())

        # df = df \
        #         .withColumn(text_column, struct([sort_column, text_column])).select(identifier_column, text_column) \
        #         .groupBy(identifier_column) \
        #         .agg(collect_list(text_column).alias(text_column)) \
        #         .withColumn(
        #             text_column,
        #             expr(
        #                 f"array_sort(transform({text_column},x->struct(x['{sort_column}'] as {sort_column},x['{text_column}'] as {text_column})))"
        #             )   
        #         ) \
        #         .withColumn(text_column, join_lines(text_column))\

        df = df.withColumn(text_column, struct([sort_column, text_column])).select(identifier_column, text_column)
        df.show(n=5, truncate=False)
        print("Completed `struct_creation`....")

        df = df.groupBy(identifier_column).agg(collect_list(text_column).alias(text_column))

        df.explain(mode="formatted")

        df.show(n=5, truncate=False)
        print("Completed `line_list_creation`....")

        df = df.withColumn(
            text_column,
            expr(
                f"array_sort(transform({text_column},x->struct(x['{sort_column}'] as {sort_column},x['{text_column}'] as {text_column})))"
            )   
        )
        df.show(n=5, truncate=False)
        print("Completed `new_column_creation`....")

        df = df.withColumn(text_column, join_lines(text_column))

        return df
    
class SparkOptimizedHandlers():

    def get_num_lines(self, grouped_line_df):
        lines_count = grouped_line_df.agg(count("*").alias("lines_count"))
        return lines_count

    def get_mean_line_length(self, grouped_line_df, line_len_col_):
        mean_line_lengths = grouped_line_df.agg(avg(line_len_col_).alias("mean_line_length"))
        return mean_line_lengths

    def get_median_line_length(self, grouped_line_df, line_len_col_):
        median_line_lengths = grouped_line_df.agg(median(line_len_col_).alias("median_line_length"))
        return median_line_lengths

    def get_mode_line_length(self, grouped_line_df, line_len_col_):
        mode_line_lengths = grouped_line_df.agg(mode(line_len_col_).alias("mode_line_length"))
        return mode_line_lengths

    def get_min_line_length(self, grouped_line_df, line_len_col_):
        min_line_lengths_col = grouped_line_df.agg(min(line_len_col_).alias("min_line_length"))
        return min_line_lengths_col

    def get_max_line_length(self, grouped_line_df, line_len_col_):
        max_line_lengths = grouped_line_df.agg(max(line_len_col_).alias("max_line_length"))
        return max_line_lengths

    def get_nsfw_words_count(self, grouped_line_df, line_nsfw_count_col_):
        nsfw_count = grouped_line_df.agg(sum(line_nsfw_count_col_).alias("nsfw_words_count"))
        return nsfw_count

    def get_symbol_numbers_count(self, grouped_line_df, line_sym_num_count_col_):
        sym_num_count = grouped_line_df.agg(sum(line_sym_num_count_col_).alias("symbol_numbers_count"))
        return sym_num_count

    def get_non_li_words_count(self, grouped_line_df, line_non_li_count_col_):
        non_li_count = grouped_line_df.agg(sum(line_non_li_count_col_).alias("non_li_char_count"))
        return non_li_count

    def get_bytes(self, grouped_line_df, line_bytes_col_):
        bytes = grouped_line_df.agg(sum(line_bytes_col_).alias("bytes"))
        return bytes

    def get_words_count(self, grouped_line_df, line_words_count_col_):
        word_count = grouped_line_df.agg(sum(line_words_count_col_).alias("words_count"))
        return word_count

    def get_char_count(self, grouped_line_df, line_char_count_col_):
        char_count = grouped_line_df.agg(sum(line_char_count_col_).alias("char_count"))
        return char_count

    def get_repeated_line_dist(self, line_df, id_col, text_col):

        col_name = "repeated_line_dist"

        repeated_line_dist = line_df.groupBy(id_col, text_col) \
                                    .agg(count("*").alias(col_name))
        repeated_line_dist.show(n=5)

        print("Completed `count by doc_id for repeated_line_dist`....")
                        
        repeated_line_dist = repeated_line_dist \
                                    .groupBy(id_col) \
                                    .agg(collect_list(
                                            create_map([text_col, col_name])) \
                                            .alias(col_name))

        repeated_line_dist.show(n=5)

        print("Completed `structure creation for repeated_line_dist`....")

        repeated_line_dist = repeated_line_dist.withColumn("keys", expr(f"transform({col_name}, x -> map_keys(x)[0])"))

        repeated_line_dist.show(n=5)

        print("Completed `creation of key column for repeated_lines_dist`....")

        repeated_line_dist = repeated_line_dist.withColumn("values", expr(f"transform({col_name}, x -> map_values(x)[0])"))

        repeated_line_dist.show(n=5)

        print("Completed `creation of value column for repeated_lines_dist`....")

        repeated_line_dist = repeated_line_dist.withColumn(col_name, map_from_arrays(col("keys"), col("values")))

        repeated_line_dist.show(n=5)

        print("Completed `creation of map from arrays for repeated_lines_dist`....")

        repeated_line_dist = repeated_line_dist.drop("keys", "values")

        repeated_line_dist.show(n=5)

        print("Completed `repeated_lines_dist`....")

        return repeated_line_dist

    def run_analysis(
        self,
        line_df,
        doc_id_col,
        text_col,
        line_nsfw_count_col_,
        line_sym_num_count_col_,
        line_non_li_count_col_,
        line_bytes_col_,
        line_words_count_col_,
        line_char_count_col_,
    ):
        
        grouped_line_df = line_df.groupBy(doc_id_col)

        num_lines_df = self.get_num_lines(grouped_line_df)
        num_lines_df.show(n=5)
        print("Completed `line_count`....")

        nsfw_words_count_df = self.get_nsfw_words_count(grouped_line_df, line_nsfw_count_col_)
        nsfw_words_count_df.show(n=5)
        print("Completed `nsfw_words_count`....")

        symbols_numbers_count_df = self.get_symbol_numbers_count(grouped_line_df, line_sym_num_count_col_)
        symbols_numbers_count_df.show(n=5)
        print("Completed `symbol_numbers_count....")

        non_li_words_count_df = self.get_non_li_words_count(grouped_line_df, line_non_li_count_col_)
        non_li_words_count_df.show(n=5)
        print("Completed `non_li_count`....")

        bytes_df = self.get_bytes(grouped_line_df, line_bytes_col_)
        bytes_df.show(n=5)
        print("Completed `bytes`....")

        words_count_df = self.get_words_count(grouped_line_df, line_words_count_col_)
        words_count_df.show(n=5)
        print("Completed `words_count`....")

        char_count_df = self.get_char_count(grouped_line_df, line_char_count_col_)
        char_count_df.show(n=5)
        print("Completed `char_count`....")

        repeated_lines_dist_df = self.get_repeated_line_dist(line_df, doc_id_col, text_col)
        repeated_lines_dist_df.show(n=5)
        print("Completed `repeated_lines_dist`....")

        mean_line_len_df = self.get_mean_line_length(grouped_line_df, "words_count")
        mean_line_len_df.show(n=5)
        print("Completed `mean_line_len`....")

        median_line_len_df = self.get_median_line_length(grouped_line_df, "words_count")
        median_line_len_df.show(n=5)
        print("Completed `median_line_len`....")

        mode_line_len_df = self.get_mode_line_length(grouped_line_df, "words_count")
        mode_line_len_df.show(n=5)
        print("Completed `mode_line_len`....")

        min_line_len_df = self.get_min_line_length(grouped_line_df, "words_count")
        min_line_len_df.show(n=5)
        print("Completed `min_line_len`....")

        max_line_len_df = self.get_max_line_length(grouped_line_df, "words_count")
        max_line_len_df.show(n=5)
        print("Completed `max_line_len`....")

        doc_df = num_lines_df.join(mean_line_len_df, [doc_id_col]) 
        doc_df.show(n=5)
        print("Completed `join num_lines and mean_line_len`....")

        doc_df = doc_df.join(median_line_len_df, [doc_id_col]) 
        doc_df.show(n=5)
        print("Completed `join median_line_len`....")

        doc_df = doc_df.join(mode_line_len_df, [doc_id_col]) 
        doc_df.show(n=5)
        print("Completed `join mode_line_len`....")

        doc_df = doc_df.join(min_line_len_df, [doc_id_col]) 
        doc_df.show(n=5)
        print("Completed `join min_line_len`....")

        doc_df = doc_df.join(max_line_len_df, [doc_id_col]) 
        doc_df.show(n=5)
        print("Completed `join max_line_len`....")

        doc_df = doc_df.join(nsfw_words_count_df, [doc_id_col]) 
        doc_df.show(n=5)
        print("Completed `join nsfw_words_count`....")

        doc_df = doc_df.join(symbols_numbers_count_df, [doc_id_col])
        doc_df.show(n=5)
        print("Completed `join symbol_words_count`....")

        doc_df = doc_df.join(non_li_words_count_df, [doc_id_col])
        doc_df.show(n=5)
        print("Completed `join non_li_words_count`....")

        doc_df = doc_df.join(bytes_df, [doc_id_col])
        doc_df.show(n=5)
        print("Completed `join bytes`....")

        doc_df = doc_df.join(words_count_df, [doc_id_col])
        doc_df.show(n=5)
        print("Completed `join words_count`....")

        doc_df = doc_df.join(char_count_df, [doc_id_col])
        doc_df.show(n=5)
        print("Completed `join char_count`....")

        doc_df = doc_df.join(repeated_lines_dist_df, [doc_id_col])
        doc_df.show(n=5)
        print("Completed `join repeated_lines_dist`....")

        # doc_df = num_lines_df \
        #         .join(mean_line_len_df, [doc_id_col]) \
        #         .join(median_line_len_df, [doc_id_col]) \
        #         .join(mode_line_len_df, [doc_id_col]) \
        #         .join(min_line_len_df, [doc_id_col]) \
        #         .join(max_line_len_df, [doc_id_col]) \
        #         .join(nsfw_words_count_df, [doc_id_col]) \
        #         .join(symbols_numbers_count_df, [doc_id_col]) \
        #         .join(non_li_words_count_df, [doc_id_col]) \
        #         .join(bytes_df, [doc_id_col]) \
        #         .join(words_count_df, [doc_id_col]) \
        #         .join(char_count_df, [doc_id_col]) \
        #         .join(repeated_lines_dist_df, [doc_id_col])
        
        return doc_df

    def run_flagging(
        self, 
        doc_df,
        word_count_col,
        char_count_col,
        nsfw_count_col,
        nsfw_threshold,
        symbol_numbers_count_col,
        symbol_numbers_threshold,
        non_li_count_col, 
        non_li_threshold,
        min_line_count,
        line_count_col,
        min_mean_line_len,
        mean_line_len_col,
    ):
        # doc_df = doc_df \
        #         .select("*", when(doc_df[line_count_col] <= min_line_count, True).otherwise(False).alias("has_less_lines")) \
        #         .select("*", when(doc_df[mean_line_len_col] <= min_mean_line_len, True).otherwise(False).alias("is_short_lines_heavy")) \
        #         .select("*", when(doc_df[nsfw_count_col]/doc_df[word_count_col] >= nsfw_threshold, True).otherwise(False).alias("is_nsfw_heavy")) \
        #         .select("*", when(doc_df[symbol_number_count_col]/doc_df[char_count_col] >= symbol_number_threshold, True).otherwise(False).alias("is_symbol_number_heavy")) \
        #         .select("*", when(doc_df[non_li_count_col]/doc_df[char_count_col] >= non_li_threshold, True).otherwise(False).alias("is_non_li_heavy"))

        doc_df = doc_df.select("*", when(doc_df[line_count_col] <= min_line_count, True).otherwise(False).alias("has_less_lines"))
        doc_df.show(n=5)
        print("Completed `has_less_lines`....")

        doc_df = doc_df.select("*", when(doc_df[mean_line_len_col] <= min_mean_line_len, True).otherwise(False).alias("is_short_lines_heavy"))
        doc_df.show(n=5)
        print("Completed `is_short_lines_heavy`....")

        doc_df = doc_df.select("*", when(doc_df[nsfw_count_col]/doc_df[word_count_col] >= nsfw_threshold, True).otherwise(False).alias("is_nsfw_heavy"))
        doc_df.show(n=5)
        print("Completed `is_nsfw_heavy`....")

        doc_df = doc_df.select("*", when(doc_df[symbol_numbers_count_col]/doc_df[char_count_col] >= symbol_number_threshold, True).otherwise(False).alias("is_symbol_number_heavy"))
        doc_df.show(n=5)
        print("Completed `is_symbol_number_heavy`....")

        doc_df = doc_df.select("*", when(doc_df[non_li_count_col]/doc_df[char_count_col] >= non_li_threshold, True).otherwise(False).alias("is_non_li_heavy"))
        doc_df.show(n=5)
        print("Completed `is_non_li_heavy`....")
        
        return doc_df
