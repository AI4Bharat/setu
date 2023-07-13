import argparse
import os
import sys
import nltk
from nltk import ngrams
from datasets import load_dataset
import glob
from collections import Counter, OrderedDict
from functools import partial
from math import sqrt
import numpy as np
import time

 
def parse_args():

    parser = argparse.ArgumentParser(description="Script for finding n-gram repetitions in a document")

    parser.add_argument(
        "--json_glob_exp",
        "-j",
        type=str,
        help="Glob expression to use for get all JSON paths",
        required=False,
        default=None,
    )

    parser.add_argument(
        "--json_list_file",
        "-l",
        type=str,
        help="File containing all the JSON text files.",
        required=False,
        default=None,
    )

    parser.add_argument(
        "--ngram_start",
        "-x",
        type=int,
        help="Start range of n-grams to check",
        required=True,
    )

    parser.add_argument(
        "--ngram_end",
        "-y",
        type=int,
        help="End range of n-grams to check",
        required=True,
    )

    parser.add_argument(
        "--type",
        "-t",
        type=str,
        required=True,
        choices=["word", "character"],
        help="Whether to use word or character ngram."
    )

    parser.add_argument(
        "--save_path",
        "-s",
        type=str,
        help="Path to folder where updates JSONs need to be saved",
        required=True,
    )

    parser.add_argument(
        "--num_proc",
        "-p",
        type=int,
        help="Number of parallel processes to deploy",
        required=False,
        default=int(os.cpu_count())
    )

    parser.add_argument(
        "--batch_size",
        "-b",
        type=int,
        help="batch-size per process",
        required=True,
    )

    args = parser.parse_args()

    return args


def calculate_character_ngram_repetition(documents, ngram_start, ngram_end):

    for n in range(ngram_start, ngram_end + 1):

        documents[f"{n}_gram_characters"] = []
        documents[f"{n}_gram_characters_freq_dist"] = []
        documents[f"{n}_gram_characters_repetition_score"] = []

        # for text in documents["text"]:
        for text in documents["body"]:
            
            n_grams = ngrams(tuple(text), n)
            n_grams = tuple(map(lambda x: "".join(x), n_grams))
            n_gram_freq_dist = Counter(n_grams)
            total_freq = n_gram_freq_dist.total()
            n_gram_freq_dist = OrderedDict(Counter(n_grams))
            documents[f"{n}_gram_characters"] += [tuple(n_gram_freq_dist.keys())]
            documents[f"{n}_gram_characters_freq_dist"] += [tuple(n_gram_freq_dist.values())]

            sorted_freq_dist = sorted(n_gram_freq_dist.items(), key=lambda x:x[1])
            k = int(sqrt(len(sorted_freq_dist)))
            sum_of_top_k = sum([sorted_freq_dist[i][1] for i in range(k)])
            
            documents[f"{n}_gram_characters_repetition_score"] += [sum_of_top_k / total_freq]

    return documents


def calculate_word_ngram_repetition(documents, ngram_start, ngram_end):

    for n in range(ngram_start, ngram_end + 1):

        documents[f"{n}_gram_words"] = []
        documents[f"{n}_gram_words_freq_dist"] = []
        documents[f"{n}_gram_words_repetition_score"] = []

        # for text in documents["text"]:
        for text in documents["body"]:

                n_grams = ngrams(text.split(), n)
                n_grams = tuple(map(lambda x: " ".join(x), n_grams))
                n_gram_freq_dist = Counter(n_grams)
                total_freq = n_gram_freq_dist.total()
                n_gram_freq_dist = OrderedDict(Counter(n_grams))
                documents[f"{n}_gram_words"] += [tuple(n_gram_freq_dist.keys())]
                documents[f"{n}_gram_words_freq_dist"] += [tuple(n_gram_freq_dist.values())]

                x = np.array(tuple(n_gram_freq_dist.values()))
                sum_of_greater_equal_2 = np.where( x >= 2, x, 0).sum()
                
                documents[f"{n}_gram_words_repetition_score"] += [sum_of_greater_equal_2 / total_freq]

    return documents


if __name__ == "__main__":

    args = parse_args()

    if args.json_glob_exp:
        json_paths = glob.glob(args.json_glob_exp)
    elif args.json_list_file:
        with open(args.json_list_file, "r") as json_list_f:
            json_paths = json_list_f.readlines()
        
    json_paths = tuple(map(lambda x: x.replace("/mnt/data/datasets/", "/mnt/phallm-data/datasets/").strip(), json_paths))
    print(json_paths[1])
    print(len(json_paths))

    # json_paths = {
    #     "train": '/mnt/phallm-data/datasets/indiccorpsv2/raws/*/*/*/*',
    # }

    start_time = time.time()

    ds = load_dataset("json", data_files=json_paths, streaming=True)

    info_dict = {
        "glob_pattern": args.json_glob_exp,
        # "retrieved_json_count": len(ds),
        "num_of_parallel_procs": args.num_proc
    }

    # print("Initial Info: ", info_dict)

    # if args.type == "word":
    #     calculate_x_y_gram_repetition = partial(calculate_word_ngram_repetition, ngram_start=args.ngram_start, ngram_end=args.ngram_end)
    # elif args.type == "character":
    #     calculate_x_y_gram_repetition = partial(calculate_character_ngram_repetition, ngram_start=args.ngram_start, ngram_end=args.ngram_end)
    # else:
    #     raise ValueError(f"Value of type argument is: `{args.type}'. It can only be `word` or `character`.")

    # ds = ds.map(calculate_x_y_gram_repetition, batched=True, batch_size=args.batch_size, num_proc=int(args.num_proc))

    end_time = time.time() - start_time

    info_dict["process_time"] = end_time

    # print(ds["train"][1])

    print(info_dict)

    # ds.to_json(args.save_path)