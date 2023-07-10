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
        "--parquet_compression",
        "-z",
        type=str,
        required=False,
        default=None,
        help="Compression format to use for parquet",
        choices=["snappy", "gzip", "brotli", None],
    )

    args = parser.parse_args()

    return args


def merge_all_csv(csv_list, parquet_compression=None):
    
    df = pd.read_csv(csv_list[0])

    for csv in csv_list[1:]:
        df_1 = pd.read_csv(csv)
        df = pd.concat([df, df_1])
        print(f"Merged: {csv}")

    df = df.reset_index()
    
    csv_save_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "csv", "index.csv")
    parquet_save_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "parquet", "index.parquet")

    df.to_csv(csv_save_path)
    df.to_parquet(parquet_save_path, compression=parquet_compression)

    return True


if __name__ == "__main__":
    args = parse_args()
    csv_list = glob.glob(args.csv_glob_exp)
    merge_all_csv(csv_list=csv_list, parquet_compression=args.parquet_compression)