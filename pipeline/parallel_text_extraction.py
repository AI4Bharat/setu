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
        "--text_root_dir",
        "-t",
        type=str,
        default=None,
        required=False,
        help="Directory where texts will be stored"
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
        "--run_parallel_crawling",
        "-c",
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


def extract_website_text(website_info, parquet_root_dir):

    print(f"Starting crawl for sangraha/crawls_29-5-2023/html/{website_info[1]}/{website_info[0]}/ .......")

    try:

        eos_client = Minio('objectstore.e2enetworks.net',
                            access_key='ZEBYMSQX84YO5SW8VIYV',
                            secret_key='YU3NO3JK2WLE37X97BRONFG3QJ643MTL70P9DPJ3',
                            secure=True)

        docs_per_website_objects = eos_client.list_objects('ai4b-public-nlu-nlg',
                                                            prefix=f'sangraha/crawls_29-5-2023/html/{website_info[1]}/{website_info[0]}/',
                                                            recursive=False)

        for obj in docs_per_website_objects:
            
            
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

    if not args.text_root_dir:
        file_dir = os.path.dirname(os.path.abspath(__file__))
        text_root_dir = os.path.join(file_dir, "trafilatura-extracted-text")
    else:
        text_root_dir = args.text_root_dir

    eos_client = Minio('objectstore.e2enetworks.net',
                        access_key='ZEBYMSQX84YO5SW8VIYV',
                        secret_key='YU3NO3JK2WLE37X97BRONFG3QJ643MTL70P9DPJ3',
                        secure=True)

    website_lang_mapping_path = args.website_lang_mapping_path

    with open(website_lang_mapping_path, "r") as openfile:
        website_lang_mapping_object = json.load(openfile)

    if args.run_parallel_text_extraction:

        parallel_text_extraction = partial(extract_website_text, text_root_dir=text_root_dir)

        with multiprocessing.Pool(check_num_proc(args.num_proc)) as extraction_pool:
            status_list = extraction_pool.map(parallel_text_extraction, tuple(website_lang_mapping_object.items()))

        status_df = pd.DataFrame(status_list)

        status_df.to_csv(os.path.join(text_root_dir, "crawl_status.parquet"))

        print(status_df)
