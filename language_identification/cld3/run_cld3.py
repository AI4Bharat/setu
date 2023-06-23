import argparse
import os
import gcld3


def parse_args():
    pass


def run_gcld3(text):

    detector = gcld3.NNetLanguageIdentifier(min_num_bytes=0, max_num_bytes=1000) # the arguments are probably how much memory to allocate in the gpu. Not sure though!

    result = detector.FindLanguage(text=text)

    return result


if __name__ == "__main__":

    args = parse_args()

    text = "This text is written in English."

    result = run_gcld3(text)

    print(result.language)