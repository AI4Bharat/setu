from typing import Dict
from datasets import load_dataset, Dataset, IterableDataset
import glob
from indicnlp.tokenize.sentence_tokenize import sentence_split

g = glob.glob("/mnt/data/nikhil/data/d1/*", recursive=True)

num_lines = dict()


def calc_num_lines(a):
    num_lines[a["title"]] = len(sentence_split(a["body"], "en"))

    return a


dataset = load_dataset(
    "json",
    data_files={"en": g},
)  # type: ignore

iterable_ds = dataset["en"].map(calc_num_lines)
print(num_lines)
