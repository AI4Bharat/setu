from flashtext import KeywordProcessor
from constants import (
    TERMINAL_PUNCTUATIONS,
    FILTERS
)


def create_kwpr(keywords):
    keyword_processor = KeywordProcessor()
    if isinstance(keywords, dict):
        keyword_processor.add_keywords_from_dict(keywords)
    elif isinstance(keywords, list):
        keyword_processor.add_keywords_from_list(keywords)
    else:
        raise Exception("Please send keywords as a dict or list")
    return keyword_processor


def init_kwprs():
    kw_processors = {}
    for lang, values in FILTERS.items():
        kw_processors[lang] = {}
        for flter, kws in values.items():
            kw_processors[lang][flter] = create_kwpr(kws)
    return kw_processors

KW_PROCESSORS = init_kwprs()


# Step 1: Filter lines without terminal punctuation
def is_terminal_punctuation(line):
    if line.endswith(TERMINAL_PUNCTUATIONS):
        return True
    return False


# Step 2: Filter lines based on word count range
def get_word_count(line):
    return len(line.split())


# Step 3: Filter lines based on junk content
def is_junk_content(line, lang):
    extracted_kws = KW_PROCESSORS[lang]["junk_partial"].extract_keywords(line)
    if extracted_kws:
        return True

    extracted_kws = KW_PROCESSORS[lang]["junk_complete"].extract_keywords(line, span_info=True)
    for word, start, end in extracted_kws:
        if start == 0 and end == len(line):
            return True
    return False


# Step 4: Filter lines containing only numbers
def is_numbers(line):
    if line.strip().isdigit():
        return True
    return False


# Step 5: Filter lines based on stop words count
def get_stop_word_dist(line, lang):
    extracted_kws = KW_PROCESSORS[lang]["stopwords"].extract_keywords(line, span_info=True)
    word_dist = {}
    for word, start, end in extracted_kws:
        if word in word_dist:
            word_dist[word] += 1
        else:
            word_dist[word] = 1
    return word_dist


def get_nsfw_word_dist(line, lang):
    extracted_kws = KW_PROCESSORS[lang]["nsfw_words"].extract_keywords(line, span_info=True)
    word_positions = [start for _, start, end in extracted_kws]
    return word_positions

