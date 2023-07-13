from indicnlp.tokenize.indic_tokenize import trivial_tokenize


def calc_word_count(d: str, lang_code: str):
    wc = dict()

    for word in trivial_tokenize(d, lang_code):
        w = word.lower().strip()
        wc[w] = wc.get(w, 0) + 1

    return wc
