from typing import Dict
from datasets import load_dataset, Dataset, IterableDataset
import glob
from indicnlp.tokenize import sentence_tokenize

g = glob.glob("/mnt/data/nikhil/data/d1/*", recursive=True)

line_lengths = dict()

# length in words and characters and bytes


def calc_line_lengths(a):
    ll = dict()

    ll["wc"] = len(a["body"].split())          # word count
    ll["cc"] = len(a["body"])                  # character count
    ll["bc"] = len(str.encode(a["body"]))      # byte count

    line_lengths[a["title"]] = ll

    return a


dataset = load_dataset(
    "json",
    data_files={"en": g},
)  # type: ignore

iterable_ds = dataset["en"].map(calc_line_lengths)
print(line_lengths)
