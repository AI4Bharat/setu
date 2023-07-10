import argparse
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "IndicLID", "Inference"))

from ai4bharat.IndicLID import IndicLID



def parse_args():
    pass


def run_indiclid(text):

    indiclid = IndicLID()
    
    results = indiclid.batch_predict([text], 1)
    
    return results


if __name__ == "__main__":

    args = parse_args()

    text = "This text is written in English."
    
    results = run_indiclid(text)

    print(results)