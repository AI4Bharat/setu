from indicnlp.tokenize.sentence_tokenize import sentence_split


def calc_num_lines(d: str, lang_code: str):
    count = len(sentence_split(d, lang_code))
    return count
