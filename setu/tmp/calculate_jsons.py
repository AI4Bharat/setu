from glob import glob
import jsonlines
import os

for lang in ["Bengali", "Gujarati", "Hindi", "Sanskrit", "Tamil", "Telugu", "Urdu"]:

    jsonl_list = glob(f"/data/priyam/sangraha/pdfs_text/{lang}/2/*.jsonl")
    print(len(jsonl_list))

    def calculate_jsons_per_jsonl(jsonl_path):
        json_count = 0
        corrupt_json = False
        # print(f"Processing JSONL: {jsonl_path}")
        with jsonlines.open(jsonl_path, "r") as jsonl_file:
            try:
                for obj in jsonl_file.iter(skip_invalid=True):
                    json_count += 1
            except Exception as e:
                corrupt_json = True
        return json_count, corrupt_json


    total_sum = 0
    corrupt_jsons = []
    for jsonl_path in jsonl_list:
        jsons_count, is_corrupt = calculate_jsons_per_jsonl(jsonl_path)
        if is_corrupt:
            corrupt_jsons += [f"{jsonl_path},{jsons_count}"]
        total_sum += jsons_count

    print(f"For {lang} - {total_sum}")
    print(f"For {lang} - {len(corrupt_jsons)}")

    with open(f"/data/priyam/sangraha/pdfs_text/corrupt_jsons/{lang}.txt", "w") as cj_l:
        jsonl_files = "\n".join(corrupt_jsons)
        cj_l.write(jsonl_files)