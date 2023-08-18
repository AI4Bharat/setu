import os
import pandas as pd

def parse_args():

    parser = argparse.ArgumentParser(description="")

    parser.add_argument(
        "-"
    )

    parser.add_argument(
        "-"
    )

    return args

def drop_columns_from_parquets(input_folder, columns_to_drop, output_folder=None):
    """
    Drops specified columns from all parquet files in the given folder.

    :param input_folder: Path to the folder containing parquet files.
    :param columns_to_drop: List of columns to drop.
    :param output_folder: Path to the folder where updated parquet files will be saved. 
                           If None, will overwrite files in the input folder.
    """
    if output_folder is None:
        output_folder = input_folder

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # List all parquet files in the folder
    parquet_files = [f for f in os.listdir(input_folder) if f.endswith('.parquet')]

    for file in parquet_files:
        path = os.path.join(input_folder, file)
        df = pd.read_parquet(path)

        # Drop the columns
        df = df.drop(columns=columns_to_drop, errors='ignore')  # errors='ignore' ensures that it doesn't raise errors if a column doesn't exist

        # Save the modified DataFrame back to parquet
        output_path = os.path.join(output_folder, file)
        df.to_parquet(output_path)

    print(f"Processed {len(parquet_files)} parquet files.")

# Example usage
input_directory = "/path/to/your/folder"  # Replace with the path to your folder containing parquet files
columns_to_remove = ['column1', 'column2']  # Replace with the names of the columns you wish to drop
drop_columns_from_parquets(input_directory, columns_to_remove)
