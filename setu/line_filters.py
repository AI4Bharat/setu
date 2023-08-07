from flashtext import KeywordProcessor
from .constants import CONSTANTS, KW_PROCESSORS
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, IntegerType, ArrayType, MapType, StringType
from indicnlp.tokenize.sentence_tokenize import sentence_split
import json
import regex

def get_stop_word_dist(line, lang):
    extracted_kws = KW_PROCESSORS[lang]["stopwords"].extract_keywords(line, span_info=True)
    word_dist = {}
    for word, _, _ in extracted_kws:
        word_dist[word] = word_dist.get(word, 0) + 1
    return word_dist

def get_nsfw_words_pos(line, lang, for_spark=True):
    extracted_kws = KW_PROCESSORS[lang]["nsfw_words"].extract_keywords(line, span_info=True)
    if for_spark:
        word_positions = [(start, end) for _, start, end in extracted_kws]
        return word_positions
    else:
        return extracted_kws

def get_nsfw_word_dist(line, lang):
    nsfw_dist = {}
    nsfw_spans = get_nsfw_words_pos(line, lang, for_spark=False)
    for word, _, _ in nsfw_spans:
        nsfw_dist[word] = nsfw_dist.get(word, 0) + 1
    return nsfw_dist

def non_li_chars_total_count(text):
    non_latin_indic = regex.findall(CONSTANTS.non_indic_non_latin_re_pattern, text)
    return len(non_latin_indic)

# Step 1: Filter lines stats
def get_word_count(line):
    return len(line.split(" "))

def get_char_count(line):
    return len(line)

def get_bytes(line):
    return len(line.encode("utf-8"))    
    
def get_nsfw_words_total_count(nsfw_words_dist):
    return sum(nsfw_words_dist.values())

def get_symbol_number_count(line, lang):
    # TODO: get language specific unicode range to handle this.
    return 0

# Step 3: Filter lines containing only numbers
def is_numbers(line, lang):
    return line.isdigit()

# Step 3: Filter lines containing only numbers
# def is_numbers(line, lang):
    # # Define dictionaries to map digits from different scripts to Arabic numerals
    # numeral_systems = {
    #     'arabic': "0123456789", # not sure if this is correct
    #     'hindi': "०१२३४५६७८९",
    #     'bengali': "০১২৩৪৫৬৭৮৯",
    #     'telugu': "౦౧౨౩౪౫౬౭౮౯",
    #     'tamil': "௦௧௨௩௪௫௬௭௮௯",# Define dictionaries to map digits from different scripts to Arabic numerals
    # numeral_systems = {
    #     'arabic': "0123456789", # not sure if this is correct
    #     'hindi': "०१२३४५६७८९",
    #     'bengali': "০১২৩৪৫৬৭৮৯",
    #     'telugu': "౦౧౨౩౪౫౬౭౮౯",
    #     'tamil': "௦௧௨௩௪௫௬௭௮௯",
    #     'kannada': "೦೧೨೩೪೫೬೭೮೯"
    # }

    # # Extract all digits from the input string
    # digits = ''.join([char for char in line if char.isdigit()])

    # # Check if the extracted digits belong to any supported Indian script
    # for script in numeral_systems.get(lang, "0123456789"):
    #     if all(digit in script for digit in digits):
    #         return True

    # return False
    #     'kannada': "೦೧೨೩೪೫೬೭೮೯"
    # }

    # # Extract all digits from the input string
    # digits = ''.join([char for char in line if char.isdigit()])

    # # Check if the extracted digits belong to any supported Indian script
    # for script in numeral_systems.get(lang, "0123456789"):
    #     if all(digit in script for digit in digits):
    #         return True

    # return False

def get_stopword_total_count(stop_word_dist):
    return sum(stop_word_dist.values())

def __get_lang_code(lang: str, lang_code: str):
    return ("urdu", "ur") if lang == "urdu" and lang_code == "urd" else (lang, lang_code)

def extract_line_metadata(
    doc_id,
    source,
    text,
    lang,
    lang_code,
    url,
):
    
    lang, lang_code = __get_lang_code(lang, lang_code)
    output = {
        "doc_id": doc_id,
        "text": text,
        "source": source,
        "words_count": get_word_count(text),
        "char_count": get_char_count(text),
        "bytes": get_bytes(text),
        "nsfw_dist": get_nsfw_word_dist(text, lang),
        "symbol_numbers_count": get_symbol_number_count(text, lang),
        "only_number": is_numbers(text, lang),
        "non_li_count": non_li_chars_total_count(text),
        "language_id": lang_code,
        "url": url,
    }
    output["nsfw_words_count"] = get_nsfw_words_total_count(output["nsfw_dist"])
    
    return output
