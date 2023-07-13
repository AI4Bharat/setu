import os
import glob
import time
import json
import multiprocessing
from pathlib import Path
# from typing import Dict, List, Tuple

# import ulid
import datasets
# import pandas as pd
from tqdm import tqdm
import plotly.express as px
from indicnlp.tokenize.sentence_tokenize import sentence_split

# from .constants import NUM_PROCESSES
from constants import NUM_PROCESSES
from .line_filters import (
    is_terminal_punctuation,
    is_junk_content,
    is_numbers,
    get_word_count,
    get_stop_word_dist,
    get_nsfw_word_dist
)
import sys

PIPELINE_ROOT_DIR = os.path.dirname(    # phallm-data-cleaning-pipeline
                        os.path.dirname(    # pipeline
                            os.path.dirname(    # line_filters
                                __file__)))
sys.path.insert(0, os.path.join(
    PIPELINE_ROOT_DIR, "pipeline"))
from language_identification.lid_pipeline import LIDPipeline


def plot_results(processed_datasets_path):
    pass
    # Read the dataset
    # process dataframes

    # read all files and calculate:
    # mean of:
    #  - term_valid
    #  - is_junk
    #  - is_num
    #  - sum of line level stopwords
    #  - sum of line level nsfw words
    #  - Word cloud of stopwords
    #  - Word cloud of nsfw
    # plot the summed histograms of:
    #  - word_distribution
    #  - stop_word_distribution
    # Doesn't make sense if these are large

    # Plot statistics using Plotly


def split_to_lines(text, lang):
    return sentence_split(text, lang)


def filter_line_level_stats(data, lid_pipeline=None):
    lang = data["metadata"]["lang"] # Document level Language Identification
    iso_code = data["metadata"]["iso_code"]

    if not lid_pipeline:
        lid_pipeline = LIDPipeline()

    input_lines = split_to_lines(data["raw_text"], iso_code)
    
    newline_cleaned_input_lines = list(map(lambda x: x.replace("\n", " "), input_lines))
    lid_res = lid_pipeline.run_batch(newline_cleaned_input_lines)
    results = []
    for j, line in enumerate(input_lines):
        results.append(_filter_line_level_stats(j, line, lang, lid_res[j]))
    return results, input_lines


def _filter_line_level_stats(line_no, line, lang, lid_res, doc_id="id"):
    line = line.strip()
    # indic_lid_score, cld3_score, nllb_score = lid_pipeline.run_single(line)
    # indic_lid_score, cld3_score, nllb_score = None, None, None

    fmt = {
        "identifier": doc_id,
        "line_no": line_no,
        "term_valid": is_terminal_punctuation(line),
        # "is_junk": is_junk_content(line, lang),
        "is_junk": is_junk_content(line, "english"),
        "is_num": is_numbers(line),
        "word_count": get_word_count(line),
        # "word_distribution": get_word_dist(line, min_words=MIN_WORDS, max_words=MAX_WORDS),  # Dict[str, int],
        # "stop_word_distribution": get_stop_word_dist(line, lang),  # Dict[str, int],
        "stop_word_distribution": get_stop_word_dist(line, lang),  # Dict[str, int],
        "language_id": lid_res,
        # "nsfw_span": get_nsfw_word_dist(line, lang),  # List[Tuple(int, int)],
        "nsfw_span": get_nsfw_word_dist(line, "english"),  # List[Tuple(int, int)],
    }
    fmt["stopword_count"] = sum(fmt["stop_word_distribution"].values())
    fmt["stop_word_distribution"] = json.dumps(fmt["stop_word_distribution"])
    fmt["nsfw_span"] = json.dumps(fmt["nsfw_span"])
    return fmt

def run(folder_path):
    all_langs = glob.glob(folder_path)
    for lang in all_langs:
        print(lang)
        if lang != "en-small.jsonl":
            continue

        dataset = datasets.load_dataset("json", data_files=lang)
        input_data = dataset["train"].to_pandas().to_dict("records")

        # Process the function using multiprocessing
        results = []
        pool = multiprocessing.Pool(NUM_PROCESSES)
        with tqdm(total=len(input_data), desc="Processing Docs") as pbar:
            for result in pool.map(filter_line_level_stats, input_data):
                results.extend(result)
                pbar.update(1)

        # Close the multiprocessing pool
        pool.close()
        pool.join()

        processed_datasets_path = Path(os.path.join("../data/lines", "en-processed.jsonl"))  # str(ulid.new())
        with open(processed_datasets_path, "w") as f:
            for r in results:
                f.write(json.dumps(r))
                f.write("\n")
        break
    # plot_results(processed_datasets_path)

def main():
    # path = "/mnt/phallm-data/datasets/indiccorpsv2/jsonls/*"
    path = "*"
    run(path)


if __name__ == "__main__":
    # main()

    text = "Stories about North Macedonia from July, 2021\nIn North Macedonia, some young people manage to build successful businesses during the COVID-19 crisis\nThree young entrepreneurs from Bitola, a city in the south of North Macedonia, provide examples of youth who have dealt with the COVID-19 crisis in a creative and positive manner.\nBy gathering knowledge, volunteers step in to save and revive the Macedonian music industry\nVasil Buraliev, the founder of the biggest publicly available nonprofit database about music from North Macedonia, talks about challenges of digital activism to promote cultural values.\nProtests in front of Czech embassy in North Macedonia target anti-Roma racism and police brutality\nThe event also evoked the memory of a local case involving the beating of ethnic Roma citizens by police in September 2020.\nNorth Macedonia creates a new national park occupying 2% of its territory\nActivists warn, however, that declaring the Shar Mountains a national park will not stop the construction of seven small hydro power plants that are already devastating the area's natural environment.\nSecond Skopje Pride Parade celebrates diversity in North Macedonia\nAttended by President Stevo Pendarovski, the thousand-strong parade showcased support for a community traditionally discriminated against, while celebrating the hope for a society with greater solidarity and justice.\nAustrian player Arnautović sparks racism controversy in Euro 2020 football game against North Macedonia\nTwitter users pointed out that the perpetrator behind the anti-Albanian incident has a surname which might indicate ethnic Albanian roots in his family."
    data = {
        "raw_text": text,
        "metadata": {
            "lang": "english",
            "iso_code": "eng",
        }
    }
    LID_PIPELINE = LIDPipeline(
        IndicLID_FTN_path='../language_identification/models/indiclid-ftn/model_baseline_roman.bin',
        IndicLID_FTR_path='../language_identification/models/indiclid-ftr/model_baseline_roman.bin',
        IndicLID_BERT_path='../language_identification/models/indiclid-bert/basline_nn_simple.pt',
        input_threshold=0.5,
        roman_lid_threshold=0.6, 
        nllb_model_path="../language_identification/models/lid218e.bin", 
        mapping_json_path="../language_identification/language_mapping.json"
    )
    print(filter_line_level_stats(data, LID_PIPELINE))