from typing import Dict
from datasets import load_dataset, Dataset, IterableDataset
import glob
from indicnlp.tokenize.sentence_tokenize import sentence_split
from indicnlp.tokenize.indic_tokenize import trivial_tokenize

g = glob.glob("/mnt/data/nikhil/data/d1/*", recursive=True)


word_count = dict()


def calc_word_count(a):
    wc = dict()

    for word in trivial_tokenize(a["body"]):
        w = word.lower().strip()
        wc[w] = wc.get(w, 0) + 1

    word_count[a["title"]] = wc

    return a


def calc_word_count_urdu(a):
    wc = dict()

    for word in trivial_tokenize(a["body"], "ur"):
        w = word.lower().strip()
        wc[w] = wc.get(w, 0) + 1

    word_count[a["title"]] = wc

    return a


dataset = load_dataset(
    "json",
    data_files={"en": g},
)  # type: ignore

iterable_ds = dataset["en"].map(calc_word_count)
print(word_count)
