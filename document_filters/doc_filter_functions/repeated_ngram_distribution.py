from collections import Counter, OrderedDict
from math import sqrt

import numpy as np
from indicnlp.tokenize.indic_tokenize import trivial_tokenize
from nltk import ngrams


def calculate_character_ngram_repetition(d: str, ngram_start: int, ngram_end: int):
    ngram_repetition = dict()

    doc = [d]

    for n in range(ngram_start, ngram_end + 1):
        ngram_repetition[f"{n}_gram_characters"] = []
        ngram_repetition[f"{n}_gram_characters_freq_dist"] = []
        ngram_repetition[f"{n}_gram_characters_repetition_score"] = []

        for text in doc:
            n_grams = ngrams(tuple(text), n)
            n_grams = tuple(map(lambda x: "".join(x), n_grams))
            n_gram_freq_dist = Counter(n_grams)
            total_freq = n_gram_freq_dist.total()
            n_gram_freq_dist = OrderedDict(Counter(n_grams))
            ngram_repetition[f"{n}_gram_characters"] += [tuple(n_gram_freq_dist.keys())]
            ngram_repetition[f"{n}_gram_characters_freq_dist"] += [
                tuple(n_gram_freq_dist.values())
            ]

            sorted_freq_dist = sorted(n_gram_freq_dist.items(), key=lambda x: x[1])
            k = int(sqrt(len(sorted_freq_dist)))
            sum_of_top_k = sum([sorted_freq_dist[i][1] for i in range(k)])

            ngram_repetition[f"{n}_gram_characters_repetition_score"] += [
                sum_of_top_k / total_freq
            ]

    return ngram_repetition


def calculate_word_ngram_repetition(
    d: str, lang_code: str, ngram_start: int, ngram_end: int
):
    ngram_repetition = dict()

    doc = [d]

    for n in range(ngram_start, ngram_end + 1):
        ngram_repetition[f"{n}_gram_words"] = []
        ngram_repetition[f"{n}_gram_words_freq_dist"] = []
        ngram_repetition[f"{n}_gram_words_repetition_score"] = []

        # for text in documents["text"]:
        for text in doc:
            n_grams = ngrams(trivial_tokenize(d, lang_code), n)
            n_grams = tuple(map(lambda x: " ".join(x), n_grams))
            n_gram_freq_dist = Counter(n_grams)
            total_freq = n_gram_freq_dist.total()
            n_gram_freq_dist = OrderedDict(Counter(n_grams))
            ngram_repetition[f"{n}_gram_words"] += [tuple(n_gram_freq_dist.keys())]
            ngram_repetition[f"{n}_gram_words_freq_dist"] += [
                tuple(n_gram_freq_dist.values())
            ]

            x = np.array(tuple(n_gram_freq_dist.values()))
            sum_of_greater_equal_2 = np.where(x >= 2, x, 0).sum()

            ngram_repetition[f"{n}_gram_words_repetition_score"] += [
                sum_of_greater_equal_2 / total_freq
            ]

    return ngram_repetition
