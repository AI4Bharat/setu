import argparse
import os
import ftfy


def parse_args():
    pass



def correct_unicode(text):

    results = ftfy.fix_text(text)

    return results



if __name__ == "__main__":

    args = parse_args()

    # text = "This text is written in English."
    # text = "रॉबर्ट फ़ॉस्ट का सबक़\nर…\nScanned by CamScanner"
    text = "ശ്രീമദ് ഭഗവദ്ഗീതാ\nശ്രീ … പോലും\nയോഗസാധനയിലേക്ക്"

    results = correct_unicode(text)

    print(results)

