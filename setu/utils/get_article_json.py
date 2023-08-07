import argparse
import os
import pyspark
import glob
import json
import multiprocessing


def get_json_list(base_directory, second_lvl="*", third_lvl="*", file_extension=".json", output_file="output.json"):

    # Construct the base path
    base_path = os.path.join(base_directory, second_lvl, third_lvl, f'*{file_extension}')

    # Use glob to extract the list of JSON files
    json_files = glob.glob(base_path)

    # Create a dictionary to store the results
    json_dict = {}

    # Iterate over the JSON files
    for file in json_files:
        # Extract the third-level folder name
        _, second_level, third_level, _ = file.split(os.sep)[-4:]
        
        # Convert the file path to its absolute path
        absolute_path = os.path.abspath(file)
        
        # Add the absolute path to the respective folder's list in the dictionary
        if third_level in json_dict:
            json_dict[third_level].append(absolute_path)
        else:
            json_dict[third_level] = [absolute_path]

    # Define the output JSON file path

    # Save the dictionary as a JSON file
    with open(output_file, 'w') as file:
        json.dump(json_dict, file, indent=4)

    print(f"JSON file saved: {output_file}")


if __name__ == "__main__":

    get_json_list("web_crawls", "malayalam", "*", "", output_file="../malayalam_output.json")
