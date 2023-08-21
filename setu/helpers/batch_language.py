import os
import argparse
from math import ceil
import glob

def parse_args():

    parser = argparse.ArgumentParser(description="Batch language wise files")

    parser.add_argument(
        "-g",
        "--glob_path",
        type=str,
        help="Glob path whose files you want to batch based on size",
        required=True,
    )

    parser.add_argument(
        "-b",
        "--bin_size",
        type=float,
        help="Size of bin to use in GB",
        required=True,
    )

    parser.add_argument(
        "-s",
        "--batch_metadata_save_file",
        type=str,
        help=".txt file where batch of metadata will be stored.",
        required=True,
    )

    args = parser.parse_args()

    return args

def get_files_and_sizes(glob_path):
    """Return a list of (filename, filesize) tuples for all files in the folder."""
    files_and_sizes = []
    # for root, dirs, files in os.walk(folder_path):
    #     for filename in files:
    #         filepath = os.path.join(root, filename)
    for filepath in glob.glob(glob_path):
            filesize = os.path.getsize(filepath)
            files_and_sizes.append((filepath, filesize))
    return sorted(files_and_sizes, key=lambda x: x[1], reverse=True)  # sort by filesize

def split_into_bins(files_and_sizes, number_of_bins):
    """Split files into bins with approximately equal total size."""
    bins = [[] for _ in range(number_of_bins)]
    bin_sizes = [0] * number_of_bins

    for filepath, filesize in files_and_sizes:
        # Find the bin with the smallest current size
        smallest_bin_index = bin_sizes.index(min(bin_sizes))
        bins[smallest_bin_index].append(filepath)
        bin_sizes[smallest_bin_index] += filesize

    return bins

def bytes_to_gb(byte_value):
    return byte_value / 1024 ** 3

def gb_to_bytes(gb_value):
    return gb_value * 1024 ** 3

def main():

    args = parse_args()

    files_and_sizes = get_files_and_sizes(args.glob_path)
    total_size = sum(size for _, size in files_and_sizes)
    num_of_bins = ceil(total_size/gb_to_bytes(args.bin_size))
    # avg_bin_size = total_size / args.num_of_bins
    avg_bin_size = total_size / num_of_bins

    print(f"Total size: {bytes_to_gb(total_size)} GB")
    print(f"Average bin size: {bytes_to_gb(avg_bin_size):.2f} GB")
    print(f"Num of bins: {num_of_bins:.2f} bins")

    bins = split_into_bins(files_and_sizes, num_of_bins)

    batch_info = "\n".join(list(map(lambda x: ",".join(x), bins)))

    os.makedirs(os.path.dirname(args.batch_metadata_save_file), exist_ok=True)
    
    with open(args.batch_metadata_save_file, "w") as f:
        f.write(batch_info.strip())

if __name__ == "__main__":
    main()
