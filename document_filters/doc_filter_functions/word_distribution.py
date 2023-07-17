from indicnlp.tokenize.indic_tokenize import trivial_tokenize


def calc_word_count(d: str, lang_code: str):
    wc = dict()
    word_segmentated_data = trivial_tokenize(d, lang_code)

    for word in word_segmentated_data:
        w = word.lower().strip()
        wc[w] = wc.get(w, 0) + 1

    return wc, word_segmentated_data
