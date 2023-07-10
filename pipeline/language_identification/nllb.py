import argparse
import os
import fasttext


def parse_args():
    pass


def run_nllb(text):

    pretrained_lang_model = "models/lid218e.bin" # Path of model file
    model = fasttext.load_model(pretrained_lang_model)
    
    predictions = model.predict(text, k=1)

    return predictions


if __name__ == "__main__":

    args = parse_args()

    text = "صباح الخير، الجو جميل اليوم والسماء صافية."

    result = run_nllb(text)

    print(result)