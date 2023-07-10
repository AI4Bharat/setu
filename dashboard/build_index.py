from minio import Minio
import os
import json
import multiprocessing
import pandas as pd
import argparse
from functools import partial
import numpy as np

def check_num_proc(num_proc: int = -1) -> int:
    """
    Check the number of processors. Return a safe-checked value.
    Parameters
    ----------
    num_proc : int, optional
        Number of processors to use, by default -1
    Returns
    -------
    int
        Number of processors to use
    Raises
    ------
    ValueError
        If the input exceeds the number of processors available
    """
    maximum: int = multiprocessing.cpu_count()
    if num_proc > maximum:
        raise ValueError(
            f"{num_proc} exceeds the maximum number ({maximum}) of processors"
        )

    if num_proc == -1:
        num_proc = maximum
    else:
        print(f"Using {num_proc} processors out of {maximum} can be slow")

    return num_proc

def parse_args():

    parser = argparse.ArgumentParser(description="")

    parser.add_argument(
        "--csv_root_dir",
        "-c",
        type=str,
        default=None,
        required=False,
        help="Directory where csvs will be stored"
    )

    parser.add_argument(
        "--num_proc",
        "-n",
        type=int,
        default=-1,
        required=False,
        help="Maximum number of CPUs to use for parallel crawling"
    )

    parser.add_argument(
        "--run_language_website_mapping",
        "-l",
        action='store_true',
        help="Whether to run language website mapping."
    )

    parser.add_argument(
        "--run_parallel_crawling",
        "-p",
        action='store_true',
        help="Whether to run parallel crawling for indexing."
    )
    
    parser.add_argument(
        "--website_lang_mapping_path",
        "-w",
        type=str,
        default=None,
        required=False,
        help="Path to website-language mapping json",
    )

    args = parser.parse_args()

    return args


def get_languages(eos_client, csv_root_dir):

    lang_objects = eos_client.list_objects('ai4b-public-nlu-nlg',
                                    prefix='sangraha/crawls_29-5-2023/html/',
                                    recursive=False)
    languages = []
    for lang in lang_objects:
        _, doc_lang = os.path.split(lang.object_name[:-1])
        languages += [doc_lang]
        os.makedirs(os.path.join(csv_root_dir, doc_lang), exist_ok=True)

    return languages

def get_website_and_language_mapping(eos_client, languages, csv_root_dir):

    lang_website_mapping = {}
    website_lang_mapping = {}

    for lang in languages:
        websites_per_lang_objects = eos_client.list_objects('ai4b-public-nlu-nlg',
                                                            prefix=f'sangraha/crawls_29-5-2023/html/{lang}/',
                                                            recursive=False)
        lang_website_mapping[lang] = []
        for websites in websites_per_lang_objects:
            _, website = os.path.split(websites.object_name[:-1])

            lang_website_mapping[lang] += [website]
            website_lang_mapping[website] = lang

    lang_website_mapping_object = json.dumps(lang_website_mapping, indent=4)
    website_lang_mapping_object = json.dumps(website_lang_mapping, indent=4)

    lang_website_mapping_path = os.path.join(csv_root_dir, "lang_website_mapping.json")
    website_lang_mapping_path = os.path.join(csv_root_dir, "website_lang_mapping.json")

    with open(lang_website_mapping_path, "w") as outfile:
        outfile.write(lang_website_mapping_object)

    with open(website_lang_mapping_path, "w") as outfile:
        outfile.write(website_lang_mapping_object)

    return {
        "lang_website_mapping_path": lang_website_mapping_path,
        "website_lang_mapping_path": website_lang_mapping_path,
    }

def crawl_website(website_info, csv_root_dir):

    print(f"Starting crawl for sangraha/crawls_29-5-2023/html/{website_info[1]}/{website_info[0]}/ .......")

    try:

        eos_client = Minio('objectstore.e2enetworks.net',
                            access_key='ZEBYMSQX84YO5SW8VIYV',
                            secret_key='YU3NO3JK2WLE37X97BRONFG3QJ643MTL70P9DPJ3',
                            secure=True)

        df_save_path = os.path.join(csv_root_dir, website_info[1], f"{website_info[0]}.csv")

        docs_per_website_objects = eos_client.list_objects('ai4b-public-nlu-nlg',
                                                            prefix=f'sangraha/crawls_29-5-2023/html/{website_info[1]}/{website_info[0]}/',
                                                            recursive=False)

        df = pd.DataFrame(columns=["identifier", "domain", "language", "type", "size_kb"])

        for obj in docs_per_website_objects:
            doc_domain, doc_identifier = os.path.split(obj.object_name)
            doc_lang, doc_domain = os.path.split(doc_domain)
            doc_type, doc_lang = os.path.split(doc_lang)

            doc_info = {
                "identifier": obj.object_name,
                "domain": doc_domain,
                "language": doc_lang,
                "type": os.path.split(doc_type)[1],
                "size_kb": round(obj.size/1024, 4)
            }
            df.loc[len(df.index)] = doc_info

        df.to_csv(df_save_path)
        print(f"Completed crawling sangraha/crawls_29-5-2023/html/{website_info[1]}/{website_info[0]}/ .......")

    except Exception as e:
        print(f"Unable to completed crawling sangraha/crawls_29-5-2023/html/{website_info[1]}/{website_info[0]}/ due to Exception: {e}")

        return {
            "status": False,
            "path": df_save_path,
            "exception": str(e),
        }

    return {
        "status": True,
        "path": df_save_path,
        "exception": None,
    }

if __name__ == '__main__':

    args = parse_args()

    if not args.csv_root_dir:
        file_dir = os.path.dirname(os.path.abspath(__file__))
        csv_root_dir = os.path.join(file_dir, "csv")
    else:
        csv_root_dir = args.csv_root_dir

    if args.run_language_website_mapping:

        eos_client = Minio('objectstore.e2enetworks.net',
                            access_key='ZEBYMSQX84YO5SW8VIYV',
                            secret_key='YU3NO3JK2WLE37X97BRONFG3QJ643MTL70P9DPJ3',
                            secure=True)
        
        languages = get_languages(eos_client, csv_root_dir)

        print("Got Languages....")

        paths = get_website_and_language_mapping(eos_client, languages, csv_root_dir)

        print(f"Got Language-Website Mapping: Paths: {paths}")    

        website_lang_mapping_path = paths["website_lang_mapping_path"]

    else:
        website_lang_mapping_path = args.website_lang_mapping_path

    with open(website_lang_mapping_path, "r") as openfile:
        website_lang_mapping_object = json.load(openfile)

    if args.run_parallel_crawling:

        parallel_website_crawling = partial(crawl_website, csv_root_dir=csv_root_dir)

        with multiprocessing.Pool(check_num_proc(args.num_proc)) as crawl_pool:
            status_list = crawl_pool.map(parallel_website_crawling, tuple(website_lang_mapping_object.items()))

        status_df = pd.DataFrame(status_list)

        status_df.to_csv(os.path.join(csv_root_dir, "crawl_status.csv"))

        print(status_df)
