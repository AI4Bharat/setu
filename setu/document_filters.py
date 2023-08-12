from typing import Dict, Tuple
from constants import CONSTANTS, KW_PROCESSORS
import re
from ftfy import fix_and_explain
from indicnlp.tokenize.indic_tokenize import trivial_tokenize
from indicnlp.tokenize.sentence_tokenize import sentence_split
from nltk import ngrams
from math import sqrt
from collections import Counter, OrderedDict
import numpy as np
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
import json
import statistics

def find_code_spans(doc_id, text):
    patterns = [
        # HTML
        (re.compile(r'<[^>]+?>.+?</[^>]+?>'), 'HTML'),
        
        # JavaScript
        (re.compile(r'(?s)function\s*?\(.*?\)\s*?\{.*?\}'), 'JavaScript'),
        
        # CSS
        (re.compile(r'(?s)\..*?\{.*?\}'), 'CSS'),
    ]
    
    spans = []

    for pattern, lang in patterns:
        for match in pattern.finditer(text):
            spans.append([match.start(), match.end()])
    return spans if len(spans) else None

def is_terminal_valid(text):
    if text.endswith(CONSTANTS.TERMINAL_PUNCTUATIONS_EXCEPTION):
        return False
    return text.endswith(CONSTANTS.TERMINAL_PUNCTUATIONS)
    
def remove_non_terminal_punc_span(chunk, is_term_valid, chunk_len_threshold):
    if is_term_valid:
        return chunk
    
    if chunk.endswith(CONSTANTS.TERMINAL_PUNCTUATIONS_EXCEPTION):
        return None

    term_punc_latest_indices = {}
    for term_punc in CONSTANTS.TERMINAL_PUNCTUATIONS:
        term_punc_latest_indices[term_punc] = chunk.rfind(term_punc)

    latest_punc, latest_punc_index = "", -1
    for punc, latest_index in term_punc_latest_indices.items():
        if latest_punc_index < latest_index:
            latest_punc_index = latest_index
            latest_punc = punc

    chunk = chunk[:latest_punc_index+1]
    if len(chunk.split(" ")) > chunk_len_threshold:
        return chunk
    else:
        return None

def __get_lang_code(lang, lang_code):
    return ("urdu", "ur") if lang == "urdu" and lang_code == "urd" else (lang, lang_code)

def split_at_terminal_punc(text, lang, lang_code):
    _, lang_code = __get_lang_code(lang, lang_code)
    return sentence_split(text, lang_code)

def split_with_delimiter(
    text,
    # delimiter_pattern=r'[.?!।॥:,؟۔](?:\n+)?'
    delimiter_pattern=r'[.?!।॥؟۔](?:\n+)?'
):
    lines = re.split(f'({delimiter_pattern})', text)
    if len(lines) % 2 == 0:
        iter_range = range(0, len(lines), 2)
        out = [lines[i]+lines[i+1] for i in iter_range]
    else:
        iter_range = range(0, len(lines) - 1, 2)
        out = [lines[i]+lines[i+1] for i in iter_range] + [lines[-1]]
    return out

def has_code(code_spans):
    if len(code_spans):
        return True
    return False

def remove_code(text, code_spans):
    if not code_spans:
        return text
    
    result = ""
    last_index = 0

    for start, end in code_spans:
        result += text[last_index:start]  # Append the substring before the current span
        last_index = end  # Update the last index to the end of the current span

    # Append the remaining substring after the last span
    result += text[last_index:]

    return result

def terminal_punc_filter(text):
    total_chunks_flagged = 0
    chunks = text.split("\n")
    cleaned_chunks = []
    for i in range(len(chunks)):
        is_term_valid = is_terminal_valid(chunks[i])
        if not is_term_valid:
            total_chunks_flagged += 1
        else:
            cleaned_chunks += [chunks[i]]

    return "\n".join(cleaned_chunks), total_chunks_flagged


def get_num_lines(line_list):
    return len(line_list)

def get_line_length_stats(line_lengths):
    return {
        "mean": statistics.mean(line_lengths),
        "median": statistics.median(line_lengths),
        "mode": statistics.mode(line_lengths),
        "min": min(line_lengths),
        "max": max(line_lengths),
    }

def get_aggregate_stats(
    line_stats_list, 
    nsfw_count_key, 
    words_count_key, 
    char_count_key,
    non_li_key, 
    bytes_key, 
    symbol_number_count_key,
):
    aggregate_stats = {}

    for line_stat in line_stats_list:
        aggregate_stats["nsfw_words_count"] = aggregate_stats.get("nsfw_words_count", 0) + line_stat[nsfw_count_key]
        aggregate_stats["symbol_numbers_count"] = aggregate_stats.get("symbol_numbers_count", 0) + line_stat[symbol_number_count_key]
        aggregate_stats["non_li_count"] = aggregate_stats.get("non_li_count", 0) + line_stat[non_li_key]
        aggregate_stats["bytes"] = aggregate_stats.get("bytes", 0) + line_stat[bytes_key]
        aggregate_stats["words_count"] = aggregate_stats.get("words_count", 0) + line_stat[words_count_key]
        aggregate_stats["char_count"] = aggregate_stats.get("char_count", 0) + line_stat[char_count_key]

    return aggregate_stats

def restructure_nsfw_dists(arr):
    arr_dict = {}
    for mapp in arr:
        for key, val in mapp.items():
            arr_dict[key] = arr_dict.get(key, 0) + val
    return arr_dict

def is_nsfw_heavy(nsfw_count, word_count, threshold):
    return True if nsfw_count/word_count >= threshold else False

def is_symbol_number_heavy(symbol_number_count, char_count, threshold):
    return True if symbol_number_count/char_count >= threshold else False

def is_non_li_heavy(count, text_len, threshold):
    return True if count/text_len >= threshold else False    

def get_repeated_line_dist(line_stats, text_key):
    line_counts: Dict[str, int] = {}
    for line in line_stats:
        line_counts[line[text_key]] = line_counts.get(line[text_key], 0) + 1
    return line_counts

def get_char_ngram_repetition(
    d: str,
    ngrams_arr,
    for_spark: bool = True,
):

    ngram_repetition = dict()

    for n in ngrams_arr:
        n_grams = ngrams(tuple(d), n)
        n_grams = tuple(map(lambda x: "".join(x), n_grams))
        n_gram_freq_dist = Counter(n_grams)
        total_freq = n_gram_freq_dist.total()
        print(f"Total {n}-character Frequeny: ", total_freq)
        n_gram_freq_dist = OrderedDict(Counter(n_grams))
        if not for_spark:
            ngram_repetition[f"{n}_gram_characters"] = tuple(n_gram_freq_dist.keys())
            ngram_repetition[f"{n}_gram_characters_freq_dist"] = tuple(n_gram_freq_dist.values())

        sorted_freq_dist = sorted(n_gram_freq_dist.items(), key=lambda x: x[1])
        k = int(sqrt(len(sorted_freq_dist)))
        sum_of_top_k = sum([sorted_freq_dist[i][1] for i in range(k)])

        score = sum_of_top_k / total_freq if total_freq else None

        ngram_repetition[f"{n}_gram_characters_repetition_score"] = getattr(score, "tolist", lambda: score)() if score else None

    return ngram_repetition

def get_word_ngram_repetition(
    d: str, 
    lang_code: str,
    ngrams_arr,
    for_spark: bool = True,
):

    ngram_repetition = dict()

    for n in ngrams_arr:
        n_grams = ngrams(trivial_tokenize(d, lang_code), n)
        n_grams = tuple(map(lambda x: " ".join(x), n_grams))
        n_gram_freq_dist = Counter(n_grams)
        total_freq = n_gram_freq_dist.total()
        print(f"Total {n}-word Frequeny: ", total_freq)
        n_gram_freq_dist = OrderedDict(Counter(n_grams))

        if not for_spark:
            ngram_repetition[f"{n}_gram_words"] = tuple(n_gram_freq_dist.keys())
            ngram_repetition[f"{n}_gram_words_freq_dist"] = tuple(n_gram_freq_dist.values())

        x = np.array(tuple(n_gram_freq_dist.values()))
        sum_of_greater_equal_2 = np.where(x >= 2, x, 0).sum()

        score = sum_of_greater_equal_2 / total_freq if total_freq else None

        ngram_repetition[f"{n}_gram_words_repetition_score"] = getattr(score, "tolist", lambda: score)() if score else None

    return ngram_repetition

def has_repetition(repetition_scores, repetition_thresholds):
    """
    Use same function for word and character n-gram repetition. 
    Just the repetition scores and thresholds will change.
    """
    flags = []
    for n_gram, repetition_score in repetition_scores.items():
        n = n_gram.split("_")[0]
        threshold = repetition_thresholds[n]
        flags += [True if repetition_score and repetition_score >= threshold else False]
    return True if sum(flags) > 0 else False

def extract_document_metadata(
    doc_id, 
    source, 
    line_stats_list, 
    lang,
    lang_code: str,
    text_key, 
    nsfw_count_key, 
    words_count_key, 
    char_count_key,
    non_li_key, 
    bytes_key, 
    symbol_number_count_key,
    word_ngrams: Tuple,
    char_ngrams: Tuple, 
    url,
):
    
    lang, lang_code = __get_lang_code(lang, lang_code)

    output = {
        "doc_id": doc_id,
        "text": "".join([line[text_key] for line in line_stats_list]),
        "source": source,
        "language_id": lang_code,
        "num_of_lines": get_num_lines(line_stats_list),
        "stats": get_aggregate_stats(
            line_stats_list,
            nsfw_count_key=nsfw_count_key, 
            words_count_key=words_count_key, 
            char_count_key=char_count_key,
            non_li_key=non_li_key, 
            bytes_key=bytes_key, 
            symbol_number_count_key=symbol_number_count_key,
        ),
        "repeated_line_dist": get_repeated_line_dist(line_stats_list, text_key),
        "url": url,
    }
    output["repeated_ngram_dist"] = {
        "word": get_word_ngram_repetition(
            output["text"], lang_code, word_ngrams, for_spark=False,
        ),
        "character": get_char_ngram_repetition(
            output["text"], char_ngrams, for_spark=False
        ),
    }
    
    line_lengths = []
    
    for line_data in line_stats_list:
        line_lengths += [line_data[words_count_key]]
    
    output["line_length_stats"] = get_line_length_stats(line_lengths)
    return output        

def perform_doc_flagging(
    doc,
    min_line_count: int = 0,
    min_mean_line_len: int = 0,
    nsfw_threshold: float = 1.0,
    symbol_number_threshold: float = 1.0,    
    non_li_threshold: float = 1.0,
    word_ngram_cum_thresholds: Dict[str, float] = {
        "6": 1.0,
        "7": 1.0,
        "8": 1.0,
        "9": 1.0
    },
    char_ngram_cum_thresholds: Dict[str, float] = {
        "5": 1.0,
        "6": 1.0,
        "7": 1.0,
        "8": 1.0
    },
):
    flags = {
        "has_less_lines": False,
        "is_short_lines_heavy": False,
        "is_nsfw_heavy": False,
        "is_symbol_number_heavy": False,
        "is_non_li_heavy": False,
        "has_word_repetition": False,
        "has_char_repetition": False,
    }

    if doc["num_of_lines"] <= min_line_count:
        flags["has_less_lines"] = True
    
    if doc["line_length_stats"]["mean"] <= min_mean_line_len:
        flags["is_short_lines_heavy"] = True

    flags["is_nsfw_heavy"] = is_nsfw_heavy(doc["stats"]["nsfw_words_count"], doc["stats"]["words_count"], nsfw_threshold)
    flags["is_symbol_number_heavy"] = is_symbol_number_heavy(doc["stats"]["symbol_numbers_count"], doc["stats"]["char_count"], symbol_number_threshold)
    flags["is_non_li_heavy"] = is_non_li_heavy(doc["stats"]["non_li_count"], doc["stats"]["char_count"], non_li_threshold)

    word_repetition_scores = {}
    for n_gram in word_ngram_cum_thresholds.keys():
        word_repetition_scores[f"{n_gram}_gram_words_repetition_score"] = doc["repeated_ngram_dist"]["word"][f"{n_gram}_gram_words_repetition_score"]
    flags["has_word_repetition"] = has_repetition(word_repetition_scores, word_ngram_cum_thresholds)

    char_repetition_scores = {}
    for n_gram in char_ngram_cum_thresholds.keys():
        char_repetition_scores[f"{n_gram}_gram_characters_repetition_score"] = doc["repeated_ngram_dist"]["character"][f"{n_gram}_gram_characters_repetition_score"]
    flags["has_character_repetition"] = has_repetition(char_repetition_scores, char_ngram_cum_thresholds)

    return flags


if __name__ == "__main__":

    text = """Till today we have got total 2911 comments from the users and total 1329 articles has been published.
(Data as on 5 March 2012)
- Updated on 19 Jan 2021
- Total visited users: 5,85,227
- Total page read: 3,034,600
- Avg. Session Duration: 6 min 6 sec
    """
    # with open('/mnt/phallm-data/priyam/setu/text.txt') as f:
        # text = f.read()

    # print(sentence_split(text, "eng"))
    # print(text)
    # print("********************")
    # print([text])

    output = split_with_delimiter(text)
    print(output) 
    # print(output[0])
    # print([output])
    # with open("/mnt/phallm-data/priyam/setu/output.txt", 'w') as f:
        # f.write(output[0])