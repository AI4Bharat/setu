import argparse
import os 
import shutil
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    split,   
    count,
    sum,
    avg,
    udf,
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
)

from pyspark.sql.types import (
    StringType, 
)



def str2bool(v:str) -> bool:
    """str2bool Returns the boolean equivalent given various string representations of the True/False values.

    Args:
        v (str): A string that might represent a boolean value.

    Raises:
        argparse.ArgumentTypeError: Error that mentions the provided value does not represent a boolean value.

    Returns:
        bool : Returns the bool equivalent of the provided value.
    """
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
    
def list_of_strings(arg:str) -> list:
    """list_of_strings Generate the list of strings provided a single string.

    Args:
        arg (str): The string argument which needs to be split.

    Returns:
        list: List of strings of the string split using ',' delimitter.
    """
    return arg.split(',')

def rename_partitioned_directories(base_dir:str, partition_column_name:str): 
    """rename_partitioned_directories Function that renames the partitioned directiories.

    Args:
        base_dir (str): Base directory path
        partition_column_name (str): Column name based on which the partitions were produced.
    """
    for dir_name in os.listdir(base_dir):
        if dir_name.startswith(partition_column_name + "="):
            new_name = dir_name.split("=")[1]
            old_path = os.path.join(base_dir, dir_name)
            new_path = os.path.join(base_dir, new_name)
            shutil.move(old_path, new_path)
    
class ChunkHandler():
    """ChunkHandler The Class representation for the a handler object that provides utilities that manipulates various chunks of text data.
    """
    def doc2lines(
            self,
            df:DataFrame,
            text_column:str,
            split_symbol:str
    ) -> DataFrame:
        """doc2lines Given a dataframe, Splits the various documents into multiple lines.

        Args:
            df (DataFrame): The dataframe object input.
            text_column (str): The column name for the text in the dataframe.
            split_symbol (str): The symbol on which splits need to be done.

        Returns:
            DataFrame: _description_
        """
        df = df \
            .withColumn(text_column, split(text_column, split_symbol, -1)) \
            .select("*", posexplode(text_column)).drop(text_column).withColumnRenamed("col", text_column) \
        
        return df
    
    def lines2doc(
            self,
            df:DataFrame,
            text_column:str,
            identifier_column:str,
            sort_column:str
    )->DataFrame:
        """lines2doc Given a dataframe, Merges the various lines into documents.

        Args:
            df (DataFrame): The dataframe object input.
            text_column (str): The column name for the text in the dataframe.
            identifier_column (str): The column based on which the lines need to be grouped into documents.
            sort_column (str): The column based on which the final dataframe needs to be sorted.

        Returns:
            DataFrame: _description_
        """
        def join_using_symbol(x):
            lines = []
            for line in x:
                if line:
                    lines += [line[text_column]]
            
            text = ""
            for line in lines:
                if len(line) >= 2 and line[0] == " " and line[1] == " ":
                    text += line[1:]
                else:
                    text += line
            return text
        
        join_lines = udf(join_using_symbol, StringType())

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
    """SparkOptimizedHandlers The Class representation for the a handler object that provides utilities that manipulates the dataframe and provides with various statistics.
    """
    def get_num_lines(self, grouped_line_df:DataFrame)->int:
        """get_num_lines Method that returns the number of lines present in the dataframe.

        Args:
            grouped_line_df (DataFrame): Dataframe object containing the grouped lines.

        Returns:
            int: Value representing the number of lines.
        """
        lines_count = grouped_line_df.agg(count("*").alias("lines_count"))
        return lines_count

    def get_mean_line_length(self, grouped_line_df:DataFrame, line_len_col_:str)->int:
        """get_mean_line_length Method that returns the mean line length of all the lines present in the dataframe.

        Args:
            grouped_line_df (DataFrame): Dataframe object containing the grouped lines.
            line_len_col_ (str): Column that represents the line length in a document.

        Returns:
            int: Value representing the mean line length.
        """
        mean_line_lengths = grouped_line_df.agg(avg(line_len_col_).alias("mean_line_length"))
        return mean_line_lengths

    def get_min_line_length(self, grouped_line_df:DataFrame, line_len_col_:str)->int:
        """get_min_line_length Method that returns the min line length of all the lines present in the dataframe.

        Args:
            grouped_line_df (DataFrame): Dataframe object containing the grouped lines.
            line_len_col_ (str): Column that represents the line length in a document.

        Returns:
            int: Value representing the min line length.
        """
        min_line_lengths_col = grouped_line_df.agg(min(line_len_col_).alias("min_line_length"))
        return min_line_lengths_col

    def get_max_line_length(self, grouped_line_df:DataFrame, line_len_col_:str)->int:
        """get_max_line_length Method that returns the max line length of all the lines present in the dataframe.

        Args:
            grouped_line_df (DataFrame): Dataframe object containing the grouped lines.
            line_len_col_ (str): Column name that contains the line length for the various document lines.

        Returns:
            int: Value representing the max line length.
        """
        max_line_lengths = grouped_line_df.agg(max(line_len_col_).alias("max_line_length"))
        return max_line_lengths

    def get_nsfw_words_count(self, grouped_line_df:DataFrame, line_nsfw_count_col_:str)->int:
        """get_nsfw_words_count Method that returns the number of NSFW words present in the dataframe.

        Args:
            grouped_line_df (DataFrame): Dataframe object containing the grouped lines.
            line_nsfw_count_col_ (str): Column name that contains the nsfw word count for the various document lines.

        Returns:
            int: Value representing the total nsfw word count.
        """
        nsfw_count = grouped_line_df.agg(sum(line_nsfw_count_col_).alias("nsfw_words_count"))
        return nsfw_count

    def get_non_li_words_count(self, grouped_line_df:DataFrame, line_non_li_count_col_:str)->int:
        """get_non_li_words_count Method that returns the number of non latin-indic words in the dataframe.

        Args:
            grouped_line_df (DataFrame): Dataframe object containing the grouped lines.
            line_non_li_count_col_ (str): Column name that contains the non-li word count for the various document lines.

        Returns:
            int: Value representing the total non-latin indic word count.
        """
        non_li_count = grouped_line_df.agg(sum(line_non_li_count_col_).alias("non_li_char_count"))
        return non_li_count

    def get_bytes(self, grouped_line_df:DataFrame, line_bytes_col_:str)->int:
        """get_bytes Method that returns the total bytes that represent the data present in the dataframe.

        Args:
            grouped_line_df (DataFrame): Dataframe object containing the grouped lines.
            line_bytes_col_ (str): Column name that contains the total bytes for the various document lines.

        Returns:
            int: Value representing the total bytes of data present in the dataframe.
        """
        bytes_ = grouped_line_df.agg(sum(line_bytes_col_).alias("bytes"))
        return bytes_

    def get_words_count(self, grouped_line_df:DataFrame, line_words_count_col_:str)->int:
        """get_words_count Method that returns the total word count present in the dataframe.

        Args:
            grouped_line_df (DataFrame): Dataframe object containing the grouped lines.
            line_words_count_col_ (str): Column name that contains the word count of the various document lines.

        Returns:
            int: Value representing the total word count in the dataframe.
        """
        word_count = grouped_line_df.agg(sum(line_words_count_col_).alias("words_count"))
        return word_count

    def get_char_count(self, grouped_line_df:DataFrame, line_char_count_col_:str)->int:
        """get_char_count Method that returns the total char count present in the dataframe.

        Args:
            grouped_line_df (DataFrame): Dataframe object containing the grouped lines.
            line_char_count_col_ (str): Column name that contains the char count of the various document lines.

        Returns:
            int: Value representing the total character count in the dataframe.
        """
        char_count = grouped_line_df.agg(sum(line_char_count_col_).alias("char_count"))
        return char_count

    def get_repeated_line_dist(self, line_df:DataFrame, id_col:str, text_col:str)->int:
        """get_repeated_line_dist Method that returns the distance between the closest repeated lines.

        Args:
            line_df (DataFrame): Dataframe object containing the lines.
            id_col (str): The column based on which the dataframe needs to be grouped by.
            text_col (str): The column name for the text in the dataframe.

        Returns:
            int: Returns the distance between repeated lines
        """
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
        line_df:DataFrame,
        doc_id_col:str,
        line_nsfw_count_col_:str,
        line_non_li_count_col_:str,
        line_bytes_col_:str,
        line_words_count_col_:str,
        line_char_count_col_:str,
        only_base_stats:bool=False,
    ) -> DataFrame:
        """run_analysis Method that runs the analysis and aggregates the various stats for the dataframe.

        Args:
            line_df (DataFrame): Dataframe object containing the lines.
            doc_id_col (str): The column based on which the dataframe needs to be grouped by.
            line_nsfw_count_col_ (str): Column name that contains the nsfw word count of the various document lines.
            line_non_li_count_col_ (str): Column name that contains the non latin-indic word count of the various document lines.
            line_bytes_col_ (str): Column name that contains the byte count of the various document lines.
            line_words_count_col_ (str): Column name that contains the word count of the various document lines.
            line_char_count_col_ (str): Column name that contains the character count of the various document lines.
            only_base_stats (bool, optional): If only return the basic statistic values. Defaults to False.

        Returns:
            DataFrame: Returns the dataframe with computed statistic values.
        """
        grouped_line_df = line_df.groupBy(doc_id_col)
        bytes_df = self.get_bytes(grouped_line_df, line_bytes_col_)
        words_count_df = self.get_words_count(grouped_line_df, line_words_count_col_)
        char_count_df = self.get_char_count(grouped_line_df, line_char_count_col_)

        doc_df = bytes_df \
                    .join(words_count_df, [doc_id_col]) \
                    .join(char_count_df, [doc_id_col])

        if not only_base_stats:
            num_lines_df = self.get_num_lines(grouped_line_df)
            nsfw_words_count_df = self.get_nsfw_words_count(grouped_line_df, line_nsfw_count_col_)
            non_li_words_count_df = self.get_non_li_words_count(grouped_line_df, line_non_li_count_col_)
            mean_line_len_df = self.get_mean_line_length(grouped_line_df, "words_count")
            min_line_len_df = self.get_min_line_length(grouped_line_df, "words_count")
            max_line_len_df = self.get_max_line_length(grouped_line_df, "words_count")

            doc_df = doc_df \
                        .join(num_lines_df, [doc_id_col]) \
                        .join(mean_line_len_df, [doc_id_col]) \
                        .join(min_line_len_df, [doc_id_col]) \
                        .join(max_line_len_df, [doc_id_col]) \
                        .join(nsfw_words_count_df, [doc_id_col]) \
                        .join(non_li_words_count_df, [doc_id_col])
        
        return doc_df

    def run_flagging(
        self, 
        doc_df:DataFrame,
        word_count_col:str,
        char_count_col:str,
        nsfw_count_col:str,
        nsfw_threshold:float,
        non_li_count_col:str, 
        non_li_threshold:float,
        min_line_count:int,
        line_count_col:str,
        min_mean_line_len:int,
        mean_line_len_col:str,
    )->DataFrame:
        """run_flagging Method that executes the flagging stage based on computed document statistics.

        Args:
            doc_df (DataFrame): The dataframe object containing the various documents.
            word_count_col (str): Column name that contains the word count of the various document lines.
            char_count_col (str): Column name that contains the character word count of the various document lines.
            nsfw_count_col (str): Column name that contains the nsfw word count of the various document lines.
            nsfw_threshold (float): Threshold value for number of NSFW words acceptable.
            non_li_count_col (str): Column name that contains the non latin-indic word count of the various document lines.
            non_li_threshold (float): Threshold value for number of non latin-indic words.
            min_line_count (int): Threshold value for minimum number of lines to constitute a document.
            line_count_col (str): Column name that contains the line count of the various documents.
            min_mean_line_len (int): Threshold value for the mean line length.
            mean_line_len_col (str): Column name that contains the mean line length of the various document lines.

        Returns:
            DataFrame: _description_
        """
        doc_df = doc_df \
                .select("*", when(doc_df[line_count_col] <= min_line_count, True).otherwise(False).alias("has_less_lines")) \
                .select("*", when(doc_df[mean_line_len_col] <= min_mean_line_len, True).otherwise(False).alias("is_short_lines_heavy")) \
                .select("*", when(doc_df[nsfw_count_col]/doc_df[word_count_col] >= nsfw_threshold, True).otherwise(False).alias("is_nsfw_heavy")) \
                .select("*", when(doc_df[non_li_count_col]/doc_df[char_count_col] >= non_li_threshold, True).otherwise(False).alias("is_non_li_heavy"))
        
        return doc_df