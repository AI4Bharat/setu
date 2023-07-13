import os
import sys
import gcld3
import fasttext
import json


class DocumentFilterPipeline():

    def __init__(self):
        pass

    
    
    def run_single(self, text):
        pass

    def run_batch(self, texts):
        pass
        
    


if __name__ == "__main__":

    lid = DocumentFilterPipeline()
    text = "صباح الخير، الجو جميل اليوم والسماء صافية."
    lid.run_single(text)

    print("\n\n Batched Results\n")

    texts = [
        "صباح الخير، الجو جميل اليوم والسماء صافية.",
        "This text is written in English.",
    ]
    lid.run_batch(texts)
