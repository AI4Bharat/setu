from typing import Dict
from datasets import load_dataset, Dataset, IterableDataset
import glob

g = glob.glob("/mnt/data/nikhil/data/d1/*", recursive=True)

char_count = dict()


def calc_word_count(a):
    cc = dict()

    for char in a["body"]:
        c = char.lower().strip()

        if c:
            cc[c] = cc.get(c, 0) + 1

    char_count[a["title"]] = cc

    return a


dataset = load_dataset(
    "json",
    data_files={"en": g},
)  # type: ignore

iterable_ds = dataset["en"].map(calc_word_count)
print(char_count)
