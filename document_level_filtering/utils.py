from typing import Dict

from constants import FILTERS
from flashtext import KeywordProcessor


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
    kw_processors: Dict[str, Dict[str, KeywordProcessor]] = {}
    for lang, values in FILTERS.items():
        kw_processors[lang] = {}
        for flter, kws in values.items():
            kw_processors[lang][flter] = create_kwpr(kws)
    return kw_processors


KW_PROCESSORS = init_kwprs()
