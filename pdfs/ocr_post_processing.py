import json
import numpy as np
import argparse
# import torch

def parse_args():

    parser = argparse.ArgumentParser(description="OCR Post-Processing")

    parser.add_argument(
        "-j",
        "--json_path",
        type=str,
        required=True,
        help="Path to OCR output JSOC that needs to be post processed"
    )

    args = parser.parse_args()

    return args

def clean_linebreaks(json_path):

    with open(json_path) as ocr_fi:
        ocr_output = json.load(ocr_fi)

    print("Inside output: ", ocr_output.keys())
    print(len(ocr_output["responses"]))
    print("Inside responses: ", ocr_output["responses"][0].keys())
    print("inside responses/fullTextAnnotation: ", ocr_output["responses"][0]["fullTextAnnotation"].keys())
    print("Inside responses/context: ", ocr_output["responses"][0]["context"].keys())
    print("Inside responses/fullTextAnnotation/pages: ", ocr_output["responses"][0]["fullTextAnnotation"]["pages"][0].keys())
    print("Inside responses/fullTextAnnotation/pages/blocks: ", ocr_output["responses"][0]["fullTextAnnotation"]["pages"][0]["blocks"][0].keys())
    print("Inside responses/fullTextAnnotation/pages/blocks/paragraphs: ", ocr_output["responses"][0]["fullTextAnnotation"]["pages"][0]["blocks"][0]["paragraphs"][0].keys())
    print("Inside responses/fullTextAnnotation/pages/blocks/paragraphs: ", ocr_output["responses"][0]["fullTextAnnotation"]["pages"][0]["blocks"][0]["paragraphs"][0]["word"])

    # for res in range(len(ocr_output["responses"])):

    #     for page in range(len(ocr_output["responses"][res]["fullTextAnnotation"]["pages"])):

    #         for block in range(len(ocr_output["responses"][res]["fullTextAnnotation"]["pages"][page]["blocks"])):




if __name__ == "__main__":

    args = vars(parse_args())

    clean_linebreaks(**args)
