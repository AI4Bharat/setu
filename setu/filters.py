from typing import Dict, Tuple
from constants import CONSTANTS,KW_PROCESSORS
import re
from indicnlp.tokenize.indic_tokenize import trivial_tokenize
from indicnlp.tokenize.sentence_tokenize import sentence_split
from indicnlp.normalize.indic_normalize import IndicNormalizerFactory
from nltk import ngrams
from math import sqrt
from collections import Counter, OrderedDict
import numpy as np
from pyspark.sql.types import (
    Row
)
import statistics
from unicodedata import normalize
import regex
from typing import Callable

def get_symbol_ratio(s:str, char_count:int, for_spark:bool=True):
    """get_symbol_ratio Returns the ratio of invalid characters, the number of invalid characters and the invalid characters.

    Args:
        s (str): Line that needs to be processed.
        char_count (int): Number of characters in the the line.
        for_spark (bool, optional): If computation required as part of spark pipeline. Defaults to True.
    """
    def is_valid_char(character):
        # Unicode ranges:
        # Latin (English): U+0041 - U+005A and U+0061 - U+007A
        # Devanagari (Hindi, Dogri, Konkani, Maithili, Marathi, Nepali, Sanskrit, Sindhi): U+0900 - U+097F
        # Bengali (covers Assamese, Bengali, Bodo, and potentially Manipuri): U+0980 - U+09FF
        # Gujarati: U+0A80 - U+0AFF
        # Gurmukhi (Punjabi): U+0A00 - U+0A7F
        # Kannada: U+0C80 - U+0CFF
        # Malayalam: U+0D00 - U+0D7F
        # Meitei (for Manipuri): U+ABC0 - U+ABFF
        # Oriya (Odia): U+0B00 - U+0B7F
        # Ol Chiki (Santhali): U+1C50 - U+1C7F
        # Arabic (covers Urdu): U+0600 - U+06FF
        # Tamil: U+0B80 - U+0BFF
        # Telugu: U+0C00 - U+0C7F
        # Arabic: U+0600 - U+06FF 
        # Arabic Supplement: U+0750 - U+077F 
        # Arabic Extended-A: U+08A0 - U+08FF 
        # Arabic Extended-B: U+0870 - U+089F 
        # Arabic Extended-C: U+10EC0 - U+10EFF 
        # Arabic Pres. Forms-A: U+FB50 - U+FDFF
        # Arabic Pres. Forms-B: U+FE70 - U+FEFF 
        # Arabic Mathematical...: U+1EE00 - U+1EEFF 
        # Indic Siyaq Numbers: U+1EC70 - U+1ECBF 
        # Ottoman Siyaq Numbers: U+1ED00 - U+1ED4F 
        # Rumi Numeral Symbols: U+10E60 - U+10E7F 
        
        pattern = (
            r'['
            r'\u0030-\u0039'
            r'\u0061-\u007A'
            r'\u0041-\u005A'
            r'\u0900-\u097F'
            r'\u0980-\u09FF'
            r'\u0A00-\u0A7F'
            r'\u0A80-\u0AFF'
            r'\u0C00-\u0C7F'
            r'\u0C80-\u0CFF'
            r'\u0D00-\u0D7F'
            r'\uABC0-\uABFF'
            r'\u0B00-\u0B7F'
            r'\u1C50-\u1C7F'
            r'\u0B80-\u0BFF'
            r'\u0600-\u06FF'
            r'\u0750-\u077F'
            r'\u08A0-\u08FF'
            r'\u0870-\u089F'
            r'\uFB50-\uFDFF'
            r'\uFE70-\uFEFF'
            r'\U00010EC0-\U00010EFF'        # for '\u', python exits at 4 hex digits and for '\U', python exits at 8 hex digits. 
            r'\U0001EE00-\U0001EEFF'
            r'\U0001EC70-\U0001ECBF'
            r'\U0001ED00-\U0001ED4F'
            r'\U00010E60-\U00010E7F'
            r']'
        )

        return re.match(pattern, character)

    invalid_characters_count = 0
    invalid_chars_found = []
    exception_list = [" ", "\n"]   
    char_counter = Counter(s)
    for char in char_counter.keys():
        if not is_valid_char(char) and char not in exception_list:
                invalid_characters_count += char_counter[char]
                invalid_chars_found += [char]
    if for_spark:
        return invalid_characters_count/char_count if char_count else None, invalid_characters_count
    return invalid_characters_count/char_count, invalid_characters_count, invalid_chars_found

def is_num_or_punc_only(s:str, threshold:float=0.4)->bool:
    """is_num_or_punc_only Function that checks if a line is either numbers or punctuation only

    Args:
        s (str): Line to be processed
        threshold (float, optional): Threshold value for checking number of numeric/punctuation characters in the line. Defaults to 0.4.

    Returns:
        bool: Boolean Value that indicates if the string had numeric or punctuation only as majority content.
    """
    if s.isnumeric():
        return True
    
    def is_valid_char(character):
        pattern = (
            r'['
            # Handles Latin
            r'\u0041-\u005A' 
            r'\u0061-\u007A'
            
            # Handles Devnagari
            r'\u0900-\u0963' 
            r'\u0970-\u097F'
            
            # Handles Bengali
            r'\u0980-\u09E3'
            r'\u09F0-\u09FF'
            
            # Handles Gurumukhi
            r'\u0A00-\u0A65'
            r'\u0A70-\u0A7F'
            
            # Handles Gujarati
            r'\u0A80-\u0AE5'
            r'\u0AF0-\u0AFF'
            
            # Handles Telugu
            r'\u0C00-\u0C65'
            r'\u0C70-\u0C7F'
            
            # Handles Kannada
            r'\u0C80-\u0CE5'
            r'\u0CF0-\u0CFF'
            
            # Handles Malayalam
            r'\u0D00-\u0D65'
            r'\u0D70-\u0D7F'
            
            # Handles Meitei
            r'\uABC0-\uABED'
            
            # Handles Oriya
            r'\u0B00-\u0B65'
            r'\u0B70-\u0B7F'
            
            # Handles Ol Chiki
            r'\u1C5A-\u1C7F'
            
            # Handles Tamil
            r'\u0B80-\u0BE5'
            r'\u0BF0-\u0BFF'
            
            # Handles Arabic
            r'\u0600-\u065F'
            r'\u0670-\u06EF'
            r'\u06FA-\u06FF'
            
            # Handles additional arabic characters
            r'\u0750-\u077F'
            r'\u08A0-\u08FF'
            r'\u0870-\u089F'
            r'\uFB50-\uFDFF'
            r'\uFE70-\uFEFF'
            r'\U00010EC0-\U00010EFF'        # for '\u', python exits at 4 hex digits and for '\U', python exits at 8 hex digits.
            r']'
        )

        return re.match(pattern, character)
    
    char_count = len(s)
    invalid_characters_count = 0
    invalid_chars_found = []
    exception_list = [" ", "\n"]   
    char_counter = Counter(s)
    for char in char_counter.keys():
        if not is_valid_char(char) and char not in exception_list:
            invalid_characters_count += char_counter[char]
            invalid_chars_found += [char]    
    invalid_ratio = invalid_characters_count/char_count if char_count else None
    
    return True if invalid_ratio and invalid_ratio >= threshold else False

patterns = [
        # HTML
        (re.compile(r'<[^>]+?>.+?</[^>]+?>'), 'HTML'),
        
        # JavaScript
        (re.compile(r'(?s)function\s*?\(.*?\)\s*?\{.*?\}'), 'JavaScript'),
        
        # CSS
        (re.compile(r'(?s)\..*?\{.*?\}'), 'CSS'),
    ]
"""The various patterns to identify if the text is HTML, JS or CSS content."""

def find_code_spans_spark(doc_id:str, text:str)->Row:
    """find_code_spans_spark Returns a Pyspark Row that contains the code content in the given text

    Args:
        doc_id (str): The document ID
        text (str): The text containing the code spans/lines.

    Returns:
        Row: A Pyspark Row object containing the code spans and if present.
    """
    spans = []
    try:
        for pattern, lang in patterns:
            for match in pattern.finditer(text):
                spans.append([match.start(), match.end()])
        return Row("code_spans", "code_spans_success")(spans if len(spans) else None, True)
    except:
        return Row("code_spans", "code_spans_success")(None, False)

def find_code_spans(doc_id:str, text:str)->list|None:
    """find_code_spans Returns a list that contains the code content in the given text

    Args:
        doc_id (str): The document ID
        text (str): The text containing the code spans/lines.

    Returns:
        list|None: List of code spans in the given text.
    """
    spans = []
    for pattern, lang in patterns:
        for match in pattern.finditer(text):
            spans.append([match.start(), match.end()])
    return spans if len(spans) else None

def is_terminal_valid(text:str)->bool:
    """is_terminal_valid Check if the line terminal is a valid value.

    Args:
        text (str): The text to be checked.

    Returns:
        bool: Boolean value indicating if the line has a valid terminal value.
    """
    if text.endswith(CONSTANTS.TERMINAL_PUNCTUATIONS_EXCEPTION):
        return False
    return text.endswith(CONSTANTS.TERMINAL_PUNCTUATIONS)
    
def remove_non_terminal_punc_span(chunk:str, is_term_valid:bool, chunk_len_threshold:float)->str|None:
    """remove_non_terminal_punc_span Function that removes non terminal punctuation spans.

    Args:
        chunk (str): The text chunk to be processed.
        is_term_valid (bool): Boolean value if terminal is valid.
        chunk_len_threshold (float): Value representing the minimum chunk length

    Returns:
        str|None: Return the text chunk after removing non terminal punctuation spans.
    """
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

def __get_lang_code(lang:str, lang_code:str)->tuple:
    """__get_lang_code Functions that returns a tuple containing language and it's ISO Code.

    Args:
        lang (str): Language Name.
        lang_code (str): Language ISO Code.

    Returns:
        tuple: Tuple containing Language, ISO Code.
    """
    return ("urdu", "ur") if lang == "urdu" and lang_code == "urd" else (lang, lang_code)

def split_at_terminal_punc(text:str, lang:str, lang_code:str)->Callable:
    """split_at_terminal_punc Split text at the terminal punctuation for the corresponding language

    Args:
        text (str): The text string.
        lang (str): Language for the text.
        lang_code (str): Language ISO Code.

    Returns:
        Callable: Returns the sentence_split for the text and language code.
    """
    _, lang_code = __get_lang_code(lang, lang_code)
    return sentence_split(text, lang_code)

def split_with_delimiter(
    text:str,
    # delimiter_pattern=r'[.?!।॥:,؟۔](?:\n+)?'
    delimiter_pattern:str=r'[.?!।|॥؟۔](?:\n+)?'
)->list:
    """split_with_delimiter Function that splits the given text based on the delimitter pattern provided

    Args:
        text (str): The text to be split.
        delimiter_pattern (str, optional): The delimitter regex pattern for splitting.

    Returns:
        list: List of the lines split based on delimitter pattern.
    """
    lines = re.split(f'({delimiter_pattern})', text)
    if len(lines) % 2 == 0:
        iter_range = range(0, len(lines), 2)
        out = [lines[i]+lines[i+1] for i in iter_range]
    else:
        iter_range = range(0, len(lines) - 1, 2)
        out = [lines[i]+lines[i+1] for i in iter_range] + [lines[-1]]
    return out

def has_code(code_spans:list)->bool:
    """has_code Check if the code_spans value is a list

    Args:
        code_spans (list): The code_spans variable

    Returns:
        bool: Boolean Value representing if the code_spans is a non-null object.
    """
    if code_spans:
        return True
    return False

def remove_code(text:str, code_spans:list)->str:
    """remove_code Functions that removes the provided code spans from the text.

    Args:
        text (str): The text string.
        code_spans (list): list of code spans that need to be removed.

    Returns:
        str: Returns the text string after removing code spans.
    """
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

def terminal_punc_filter(text:str):
    """terminal_punc_filter Function that filters out punctuations from the text provided.

    Args:
        text (str): The text string.
    """
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

def normalize_text(
    text:str, 
    lang:str,
    remove_nuktas:bool=False,
    nasals_mode:str='do_nothing',
    do_normalize_chandras:bool=False,
    do_normalize_vowel_ending:bool=False
)->Callable:
    """normalize_text Function that normalizes the given text utilizing the IndicNormalizerFactory.

    Args:
        text (str): The text string.
        lang (str): The text language.
        remove_nuktas (bool, optional): Whether to remove nuktas. Defaults to False.
        nasals_mode (str, optional): Not Defined. Defaults to 'do_nothing'.
        do_normalize_chandras (bool, optional): Whether to normalize chandras. Defaults to False.
        do_normalize_vowel_ending (bool, optional): Whether to normalize vowel endings. Defaults to False.

    Returns:
        Callable: Return Normalize Function call on the text given the language value.
    """
    normalizer_lang = {
        "assamese": "as",
        "bengali": "bn",
        "bodo": "hi",
        "dogri": "hi",
        "english": None,
        "gujarati": "gu",
        "hindi": "hi",
        "kannada": "kn",
        "kashmiri": "ur",
        "konkani": "kK",
        "maithili": "hi",
        "malayalam": "ml",
        "marathi": "mr",
        "manipuri": None,
        "nepali": "ne",
        "oriya": "or",
        "punjabi": "pa",
        "sanskrit": "sa",
        "santhali": None,
        "sindhi": "ur",
        "tamil": "ta",  
        "telugu": "te",
        "urdu": "ur",
    }

    factory = IndicNormalizerFactory()
    if lang not in ["english", "manipuri", "santhali", "other"]:
        normalizer = factory.get_normalizer(
            language=normalizer_lang[lang],
            # remove_nuktas=remove_nuktas,
            # nasals_mode=nasals_mode,
            # do_normalize_chandras=do_normalize_chandras,
            # do_normalize_vowel_ending=do_normalize_vowel_ending
        )
        return normalize('NFKC', normalizer.normalize(text))
    else:
        return normalize('NFKC', text)

def get_num_lines(line_list:list)->int:
    """get_num_lines Returns the number of lines in the line list.

    Args:
        line_list (list): List of lines.

    Returns:
        int: Value representing the number of lines in the line list.
    """
    return len(line_list)

def get_line_length_stats(line_lengths:list)->dict:
    """get_line_length_stats Function that generates a dictionary of line length statistics.

    Args:
        line_lengths (list): List of all the line lengths.

    Returns:
        dict: Dictionary containing the line length statistics.
    """
    return {
        "mean": statistics.mean(line_lengths),
        "median": statistics.median(line_lengths),
        "mode": statistics.mode(line_lengths),
        "min": min(line_lengths),
        "max": max(line_lengths),
    }

def get_aggregate_stats(
    line_stats_list:list,  
    nsfw_count_key:str, 
    words_count_key:str, 
    char_count_key:str,
    non_li_key:str, 
    bytes_key:str, 
    symbol_number_count_key:str,
)->dict:
    """get_aggregate_stats Function that aggregates all the line statistics.

    Args:
        line_stats_list (list): List of all the line stats.
        nsfw_count_key (str): Key for NSFW Count
        words_count_key (str): Key for Word Count
        char_count_key (str): Key for Character Count
        non_li_key (str): Key for Non Latin-Indic Count
        bytes_key (str): Key for bytes Count
        symbol_number_count_key (str): Key for Symbol/Number Count

    Returns:
        dict: Dictionary containing all the aggregate statistics.
    """
    aggregate_stats = {}

    for line_stat in line_stats_list:
        aggregate_stats["nsfw_words_count"] = aggregate_stats.get("nsfw_words_count", 0) + line_stat[nsfw_count_key]
        aggregate_stats["symbol_numbers_count"] = aggregate_stats.get("symbol_numbers_count", 0) + line_stat[symbol_number_count_key]
        aggregate_stats["non_li_count"] = aggregate_stats.get("non_li_count", 0) + line_stat[non_li_key]
        aggregate_stats["bytes"] = aggregate_stats.get("bytes", 0) + line_stat[bytes_key]
        aggregate_stats["words_count"] = aggregate_stats.get("words_count", 0) + line_stat[words_count_key]
        aggregate_stats["char_count"] = aggregate_stats.get("char_count", 0) + line_stat[char_count_key]

    return aggregate_stats

def restructure_nsfw_dists(arr:list)->dict:
    """restructure_nsfw_dists Function to restructure the NSFW Dictionary.

    Args:
        arr (list): List containing language NSFW dictionaries

    Returns:
        dict: Restructured NSFW Dictionary
    """
    arr_dict = {}
    for mapp in arr:
        for key, val in mapp.items():
            arr_dict[key] = arr_dict.get(key, 0) + val
    return arr_dict

def is_nsfw_heavy(nsfw_count:int, word_count:int, threshold:float)->bool:
    """Returns Boolean Value that indicates if the provided values corresponde to nsfw heavy ratio."""
    return True if nsfw_count/word_count >= threshold else False

def is_symbol_number_heavy(symbol_number_count:int, char_count:int, threshold:float)->bool:
    """Returns Boolean Value that indicates if the provided values corresponde to symbol heavy ratio."""
    return True if symbol_number_count/char_count >= threshold else False

def is_non_li_heavy(count:int, text_len:int, threshold:float)->bool:
    """Returns Boolean Value that indicates if the provided values corresponding to non latin indic heavy ratio."""
    return True if count/text_len >= threshold else False    

def get_repeated_line_dist(line_stats:list, text_key:str)->Dict[str,int]:
    """Returns a dict (str,int) containing repeated line and there distances"""
    line_counts: Dict[str, int] = {}
    for line in line_stats:
        line_counts[line[text_key]] = line_counts.get(line[text_key], 0) + 1
    return line_counts

def get_char_ngram_repetition(
    d: str,
    ngrams_arr,
    for_spark: bool = True,
)->dict:
    """Returns a dict contain char ngram repetition scores"""
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

        sorted_freq_dist = sorted(n_gram_freq_dist.items(), key=lambda x: x[1], reverse=True)
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
)->dict:
    """Returns a dict contain word ngram repetition scores"""
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

def has_repetition(repetition_scores:dict, repetition_thresholds:dict)->bool:
    """
    Checks if the scores correspond to repetitive n-grams or not.
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
    doc_id:str, 
    source:str, 
    line_stats_list:list, 
    lang:str,
    lang_code: str,
    text_key:str, 
    nsfw_count_key:str, 
    words_count_key:str, 
    char_count_key:str,
    non_li_key:str, 
    bytes_key:str, 
    symbol_number_count_key:str,
    word_ngrams: Tuple,
    char_ngrams: Tuple, 
    url,
)->dict:
    """Returns extracted document metadata"""
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
    doc:dict,
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
)->dict:
    """Returns document flags with corresponding boolean values"""
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
    
def get_stop_word_dist(line:str, lang:str)->dict:
    """Returns stop word distances"""
    extracted_kws = KW_PROCESSORS[lang]["stopwords"].extract_keywords(line, span_info=True)
    word_dist = {}
    for word, _, _ in extracted_kws:
        word_dist[word] = word_dist.get(word, 0) + 1
    return word_dist

def get_nsfw_words_pos(line:str, lang:str, for_spark:bool=True)->list:
    """Returns NSFW word positions in line"""
    extracted_kws = KW_PROCESSORS[lang]["nsfw_words"].extract_keywords(line, span_info=True)
    if for_spark:
        word_positions = [(start, end) for _, start, end in extracted_kws]
        return word_positions
    else:
        return extracted_kws

def get_nsfw_word_dist(line:str, lang:str)->dict:
    """Returns NSFW word distances"""
    nsfw_dist = {}
    nsfw_spans = get_nsfw_words_pos(line, lang, for_spark=False)
    for word, _, _ in nsfw_spans:
        nsfw_dist[word] = nsfw_dist.get(word, 0) + 1
    return nsfw_dist

def non_li_chars_total_count(text:str)->int:
    """Returns Non Latin-Indic word count"""
    non_latin_indic = regex.findall(CONSTANTS.non_indic_non_latin_re_pattern, text)
    return len(non_latin_indic)

# Step 1: Filter lines stats
def get_word_count(line:str)->int:
    """Returns line word count"""
    return len(line.split(" "))

def get_char_count(line:str)->int:
    """Returns line character count"""
    return len(line)

def get_bytes(line:str)->int:
    """Returns line bytes count"""
    return len(line.encode("utf-8"))    
    
def get_nsfw_words_total_count(nsfw_words_dist)->int:
    """Returns NSFW word count"""
    return sum(nsfw_words_dist.values())

# Step 3: Filter lines containing only numbers
def is_numbers(line:str, lang:str)->bool:
    """Returns True if line has only numbers"""
    return line.isdigit()

def get_stopword_total_count(stop_word_dist:dict)->int:
    """Returns stopword total count"""
    return sum(stop_word_dist.values())

def __get_lang_code(lang: str, lang_code: str):
    """__get_lang_code Functions that returns a tuple containing language and it's ISO Code.

    Args:
        lang (str): Language Name.
        lang_code (str): Language ISO Code.

    Returns:
        tuple: Tuple containing Language, ISO Code.
    """
    return ("urdu", "ur") if lang == "urdu" and lang_code == "urd" else (lang, lang_code)

def extract_line_metadata(
    doc_id:str,
    source:str,
    text:str,
    lang:str,
    lang_code:str,
    url:str,
)->dict:
    """Returns line associated stats and metadata"""
    lang, lang_code = __get_lang_code(lang, lang_code)
    output = {
        "doc_id": doc_id,
        "text": text,
        "source": source,
        "words_count": get_word_count(text),
        "char_count": get_char_count(text),
        "bytes": get_bytes(text),
        "nsfw_dist": get_nsfw_word_dist(text, lang),
        "only_number": is_numbers(text, lang),
        "non_li_count": non_li_chars_total_count(text),
        "language_id": lang_code,
        "url": url,
    }
    output["nsfw_words_count"] = get_nsfw_words_total_count(output["nsfw_dist"])
    
    return output