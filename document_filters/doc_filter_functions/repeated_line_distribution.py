from typing import Dict

from indicnlp.tokenize.sentence_tokenize import sentence_split


def calc_repeated_line_distribution(d: str, lang_code: str):
    line_counts: Dict[str, int] = {}
    for line in sentence_split(d, lang_code):
        line_counts[line] = line_counts.get(line, 0) + 1

    return line_counts
