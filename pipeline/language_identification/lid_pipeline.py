import os
import sys
import gcld3
import fasttext
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "IndicLID", "Inference"))
from ai4bharat.IndicLID import IndicLID


class LIDPipeline():

    def __init__(self):
        self.indiclid = IndicLID()
        self.cld3_detector = gcld3.NNetLanguageIdentifier(min_num_bytes=0, max_num_bytes=1000) # the arguments are probably how much memory to allocate in the gpu. Not sure though!

        nllb_pretrained_lang_model = "models/lid218e.bin" # Path of model file
        self.nllb_detector = fasttext.load_model(nllb_pretrained_lang_model)

    def run_indiclid(self, texts):
        results = self.indiclid.batch_predict(texts, len(texts))
        return results
        
    def run_cld3(self, texts):
        results = []
        for text in texts:
            results += [self.cld3_detector.FindLanguage(text=text)]
        return results

    def run_nllb(self, texts):
        predictions = self.nllb_detector.predict(texts, k=1)
        return predictions
    
    def run_single(self, text):
        indiclid_results = self.run_indiclid([text])
        cld3_results = self.run_cld3([text])
        nllb_results = self.run_nllb([text]) 

        print(indiclid_results)
        print(cld3_results)
        print(nllb_results)

    def run_batch(self, texts):
        indiclid_results = self.run_indiclid(texts)
        cld3_results = self.run_cld3(texts)
        nllb_results = self.run_nllb(texts) 

        print(indiclid_results)
        print(cld3_results)
        print(nllb_results)
        
    def hard_vote(self, indiclid_res, cld3_res, nllb_res):
        pass

    def soft_vote(self, indiclid_res, cld3_res, nllb_res):
        pass


if __name__ == "__main__":

    lid = LIDPipeline()
    text = "صباح الخير، الجو جميل اليوم والسماء صافية."
    lid.run_single(text)

    texts = [
        "صباح الخير، الجو جميل اليوم والسماء صافية.",
        "This text is written in English.",
    ]
    lid.run_batch(texts)
