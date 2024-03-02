import os
import argparse
from math import ceil
import glob
from core import SetuStage, str2bool

class BatchCreationStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

    @staticmethod
    def add_cmdline_args(parser):
        # TODO: Add command line arguments
        pass


    def run_stage_parallelized(self, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def bytes_to_gb(byte_value):
        return byte_value / 1024 ** 3

    @staticmethod
    def gb_to_bytes(gb_value):
        return gb_value * 1024 ** 3

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

    def run_data_parallelized(self, **kwargs):
        raise NotImplementedError()

    def run_local(
        self,
        glob_path,
        bin_size,
        batch_metadata_save_folder
    ):
        files_and_sizes = get_files_and_sizes(glob_path)
        total_size = sum(size for _, size in files_and_sizes)

        print(f"Input Bin Size: {gb_to_bytes(bin_size):.2f}")
        print(f"Total size: {bytes_to_gb(total_size)} GB")

        num_of_bins = ceil(total_size/gb_to_bytes(bin_size))
        avg_bin_size = total_size / num_of_bins
        
        print(f"Average bin size: {bytes_to_gb(avg_bin_size):.2f} GB")
        print(f"Num of bins: {num_of_bins:.2f} bins")

        bins = split_into_bins(files_and_sizes, num_of_bins)

        print(f"Actual bins count: {len(bins):.2f} bins")

        os.makedirs(batch_metadata_save_folder, exist_ok=True)
        
        save_paths = []

        for i, batch in enumerate(bins):
            batch_string = "\n".join(batch)
            batch_filename = os.path.join(batch_metadata_save_folder, f"batch_{i}.info")
            with open(batch_filename, "w") as batch_f:
                batch_f.write(batch_string)

            save_paths += [batch_filename]

        batch_metadata_save_file = os.path.join(batch_metadata_save_folder, "batchs.info")

        batch_info = "\n".join(save_paths)

        with open(batch_metadata_save_file, "w") as f:
            f.write(batch_info)


    def run(self, mode="local", **kwargs):
        if mode == "local":
            return self.run_local(**kwargs)
        elif mode == "gcp":
            return self.run_gcp(**kwargs)
        else:
            raise ValueError("`mode` only supports 2 values currently: `local` & `gcp`")

        



