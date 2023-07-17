from typing import Dict
from constants import Constants
from flashtext import KeywordProcessor
import re
from ftfy import fix_and_explain
from indicnlp.tokenize.indic_tokenize import trivial_tokenize
from indicnlp.tokenize.sentence_tokenize import sentence_split
from nltk import ngrams
from math import sqrt
from collections import Counter, OrderedDict
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    BooleanType,
    IntegerType, 
    ArrayType, 
    MapType, 
    StringType, 
    FloatType,
)
from functools import partial

class DocumentFiltersPipeline():

    def __init__(self, filter_data_root):
        self.constants = Constants(
            filter_data_root=filter_data_root
        )
        self.KW_PROCESSORS = self.init_kwprs()

    def create_kwpr(self, keywords):
        keyword_processor = KeywordProcessor()
        if isinstance(keywords, dict):
            keyword_processor.add_keywords_from_dict(keywords)
        elif isinstance(keywords, list):
            keyword_processor.add_keywords_from_list(keywords)
        else:
            raise Exception("Please send keywords as a dict or list")
        return keyword_processor


    def init_kwprs(self):
        kw_processors = {}
        for lang, values in self.constants.FILTER_WORDS.items():
            kw_processors[lang] = {}
            for flter, kws in values.items():
                kw_processors[lang][flter] = self.create_kwpr(kws)
        return kw_processors

    def calc_char_distribution(self, d: str):
        cc = dict()

        for char in d:
            c = char.lower().strip()
            cc[c] = cc.get(c, 0) + 1

        return cc

    def has_code(self, text):
        # List of common programming keywords
        programming_keywords = ["for", "while", "if", "else", "class", "function", "def", "return", "import", "include"]

        # Regular expression to detect code-like patterns
        # Example: 'for i in range(10):', 'int a = 5;', 'print("Hello, world!")'
        code_pattern = re.compile(r'\b(?:for|if|while|def|class|return|import|include)\b|[{};()]|\w+\(.*\)|=\s*\S+')

        # Regular expression to detect programming comments
        # Example: '// This is a comment', '/* comment */', '# comment'
        comment_pattern = re.compile(r'//.*|/\*.*\*/|#.*')

        # Detect if text contains programming keywords
        keyword_count = 0
        for keyword in programming_keywords:
            keyword_count += text.count(keyword)
            if keyword_count >= 3:
                keyword_present = True
                break
        else:
            keyword_present = False

        # Detect if text contains code patterns
        code_matches = re.finditer(code_pattern, text)
        code_indices = [(match.start(), match.end()) for match in code_matches]

        # Detect if text contains comments
        comment_matches = re.finditer(comment_pattern, text)
        comment_indices = [(match.start(), match.end()) for match in comment_matches]

        if code_indices and comment_indices:
            return "code_and_comment", code_indices, comment_indices
        elif code_indices:
            return "code", code_indices, None
        elif comment_indices:
            return "comment", None, comment_indices
        elif keyword_present:
            return "keyword", None, None
        else:
            return None, None, None
        

    def calc_incorrect_unicode(self, text):
        _, explain = fix_and_explain(text)

        incorrect_unicodes = 0 if not explain else len(explain)
        return incorrect_unicodes

    # length in words and characters and bytes
    def calc_line_lengths(self, d: str, lang_code: str):
        ll = [
            {
                "wc": len(trivial_tokenize(line, lang_code)),  # word count
                "cc": len(line),  # character count
                "bc": len(str.encode(line)),  # byte count
            }
            for line in sentence_split(d, lang_code)
        ]

        return ll

    def calc_nsfw_word_distribution(self, d: str, lang_code: str):
        nsfw_words: Dict[str, int] = {}
        extracted_kws = self.KW_PROCESSORS[lang_code]["nsfw_words"].extract_keywords(
            d, span_info=True
        )

        for kw, _, _ in extracted_kws:
            nsfw_words[kw] = nsfw_words.get(kw, 0) + 1

        return nsfw_words

    def calc_num_lines(self, d: str, lang_code: str):
        count = len(sentence_split(d, lang_code))
        return count


    def calc_repeated_line_distribution(self, d: str, lang_code: str):
        line_counts: Dict[str, int] = {}
        for line in sentence_split(d, lang_code):
            line_counts[line] = line_counts.get(line, 0) + 1

        return line_counts


    def calculate_character_ngram_repetition(
        self, d: str, ngram_start: int, ngram_end: int,
        for_spark: bool = True,
    ):
        ngram_repetition = dict()

        for n in range(ngram_start, ngram_end + 1):
            n_grams = ngrams(tuple(d), n)
            n_grams = tuple(map(lambda x: "".join(x), n_grams))
            n_gram_freq_dist = Counter(n_grams)
            total_freq = n_gram_freq_dist.total()
            n_gram_freq_dist = OrderedDict(Counter(n_grams))
            if not for_spark:
                ngram_repetition[f"{n}_gram_characters"] = tuple(n_gram_freq_dist.keys())
                ngram_repetition[f"{n}_gram_characters_freq_dist"] = tuple(n_gram_freq_dist.values())

            sorted_freq_dist = sorted(n_gram_freq_dist.items(), key=lambda x: x[1])
            k = int(sqrt(len(sorted_freq_dist)))
            sum_of_top_k = sum([sorted_freq_dist[i][1] for i in range(k)])

            ngram_repetition[f"{n}_gram_characters_repetition_score"] = sum_of_top_k / total_freq

        return ngram_repetition


    def calculate_word_ngram_repetition(
        self, d: str, lang_code: str, ngram_start: int, ngram_end: int,
        for_spark: bool = True,
    ):

        ngram_repetition = dict()

        for n in range(ngram_start, ngram_end + 1):
            n_grams = ngrams(trivial_tokenize(d, lang_code), n)
            n_grams = tuple(map(lambda x: " ".join(x), n_grams))
            n_gram_freq_dist = Counter(n_grams)
            total_freq = n_gram_freq_dist.total()
            n_gram_freq_dist = OrderedDict(Counter(n_grams))

            if not for_spark:
                ngram_repetition[f"{n}_gram_words"] = tuple(n_gram_freq_dist.keys())
                ngram_repetition[f"{n}_gram_words_freq_dist"] = tuple(n_gram_freq_dist.values())

            x = np.array(tuple(n_gram_freq_dist.values()))
            sum_of_greater_equal_2 = np.where(x >= 2, x, 0).sum()

            ngram_repetition[f"{n}_gram_words_repetition_score"] = sum_of_greater_equal_2 / total_freq

        return ngram_repetition

    def calc_word_count(self, d: str, lang_code: str):
        wc = dict()
        word_segmentated_data = trivial_tokenize(d, lang_code)

        for word in word_segmentated_data:
            w = word.lower().strip()
            wc[w] = wc.get(w, 0) + 1

        return wc#, word_segmentated_data
    

    def __get_lang_code(self, lang: str, lang_code: str):
        return ("urdu", "ur") if lang == "urdu" and lang_code == "urd" else (lang, lang_code)


    def document_level_filters(self, data: Dict[str, str], lang:str, lang_code: str, ngram_start: int, ngram_end: int):
        lang, lang_code = self.__get_lang_code(lang, lang_code)
        word_distribution, word_segmented_data = self.calc_word_count(data["body"], lang_code)

        output = {
            "identifier": 0,
            "source": data["source"],
            "text": sentence_split(data["body"], lang_code),
            "word_distribution": word_distribution,
            "line_lengths": self.calc_line_lengths(data["body"], lang_code),
            "language_id": lang_code,
            "num_lines": self.calc_num_lines(data["body"], lang_code),
            "num_of_bad_unicodes": self.calc_incorrect_unicode(data["body"]),
            "char_distribution": self.calc_char_distribution(data["body"]),
            "nsfw_word_distribution": self.calc_nsfw_word_distribution(data["body"], lang),
            "repeated_line_distribution": self.calc_repeated_line_distribution(
                data["body"], lang_code
            ),
            "repeated_ngram_distribution": {
                "word": self.calculate_word_ngram_repetition(
                    data["body"], lang_code, ngram_start, ngram_end, for_spark=False,
                ),
                "char": self.calculate_character_ngram_repetition(
                    data["body"], ngram_start, ngram_end, for_spark=False
                ),
            },
        }

        return output, word_segmented_data
    

class DocumentFiltersSparkPipeline(DocumentFiltersPipeline):

    def __init__(self, filter_data_root):

        super().__init__(filter_data_root)

        self.calc_char_distribution_udf = udf(self.calc_char_distribution, MapType(StringType(), IntegerType()))
        self.has_code_udf = udf(self.has_code, BooleanType())
        self.calc_incorrect_unicode_udf = udf(self.calc_incorrect_unicode, IntegerType())
        self.calc_line_lengths_udf = udf(self.calc_line_lengths, ArrayType(MapType(StringType(), IntegerType())))
        self.calc_nsfw_word_distribution_udf = udf(self.calc_nsfw_word_distribution, MapType(StringType(), IntegerType()))
        self.calc_num_lines_udf = udf(self.calc_num_lines, IntegerType())
        self.calc_repeated_line_distribution_udf = udf(self.calc_repeated_line_distribution, MapType(StringType(), IntegerType()))
        self.calculate_character_ngram_repetition_udf = udf(self.calculate_character_ngram_repetition, MapType(StringType(), FloatType()))
        self.calculate_word_ngram_repetition_udf = udf(self.calculate_word_ngram_repetition, MapType(StringType(), FloatType()))
        self.calc_word_count_udf = udf(self.calc_word_count, MapType(StringType(), IntegerType()))

    def run_spark_pipeline(
        self, df, text_column, lid_column,
        
        
    ):

        df = df \
            .withColumn("char_distribution", self.calc_char_distribution_udf(text_column)) \
            .withColumn("has_code", self.has_code_udf(df[text_column])) \
            .withColumn("incorrect_unicode_count", self.calc_incorrect_unicode_udf(df[text_column])) \
            .withColumn("line_lengths", self.calc_line_lengths_udf(text_column, lid_column)) \
            .withColumn("nsfw_word_distribution", self.calc_nsfw_word_distribution_udf(text_column, lid_column)) \
            .withColumn("num_lines", self.calc_num_lines_udf(text_column, lid_column)) \
            .withColumn("repeated_line_distribution", self.calc_repeated_line_distribution_udf(text_column, lid_column)) \
            .withColumn("character_ngram_repetition", self.calculate_character_ngram_repetition_udf(text_column)) \
            .withColumn("word_ngram_repetition", self.calculate_word_ngram_repetition_udf(text_column, lid_column)) \
            .withColumn("word_count", self.calc_word_count_udf(text_column, lid_column))
        
        return df