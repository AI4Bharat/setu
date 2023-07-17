from typing import Dict

from ..utils import init_kwprs

KW_PROCESSORS = init_kwprs()


def calc_nsfw_word_distribution(d: str, lang_code: str):
    nsfw_words: Dict[str, int] = {}
    extracted_kws = KW_PROCESSORS[lang_code]["nsfw_words"].extract_keywords(
        d, span_info=True
    )

    for kw, _, _ in extracted_kws:
        nsfw_words[kw] = nsfw_words.get(kw, 0) + 1

    return nsfw_words
