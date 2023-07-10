import glob
import argparse
import pandas as pd
import os


def parse_args():

    parser = argparse.ArgumentParser(description="convert csvs to parquet")

    parser.add_argument(
        "--csv_glob_exp",
        "-c",
        type=str,
        required=True,
        help="Glob expression to use for csvs"
    )

    parser.add_argument(
        "--compression",
        "-z",
        type=str,
        required=False,
        default=None,
        help="Compression format to use for parquet",
        choices=["snappy", "gzip", "brotli", None],
    )

    args = parser.parse_args()

    return args


def convert_to_parquet(csv_list, compression=None):
    
    for csv in csv_list:
        parquet_save_path = csv.replace("/csv/", "/parquet/").replace(".csv", ".parquet")
        os.makedirs(os.path.dirname(parquet_save_path), exist_ok=True)
        df = pd.read_csv(csv)
        df.to_parquet(parquet_save_path, compression=compression) 
        print(f"Parquet file saved at: {parquet_save_path}")

    return True


if __name__ == "__main__":
    args = parse_args()
    csv_list = glob.glob(args.csv_glob_exp)
    convert_to_parquet(csv_list=csv_list, compression=args.compression)