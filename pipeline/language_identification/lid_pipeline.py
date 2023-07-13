import os
import sys
import gcld3
import fasttext
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "IndicLID", "Inference"))
from ai4bharat.IndicLID import IndicLID
import json


class LIDPipeline():

    def __init__(self,
                 IndicLID_FTN_path = 'models/indiclid-ftn/model_baseline_roman.bin',
                 IndicLID_FTR_path = 'models/indiclid-ftr/model_baseline_roman.bin',
                 IndicLID_BERT_path = 'models/indiclid-bert/basline_nn_simple.pt',
                 input_threshold = 0.5, roman_lid_threshold = 0.6, 
                 nllb_model_path="models/lid218e.bin", 
                 mapping_json_path="./language_mapping.json"):
        
        self.indiclid = IndicLID(
            IndicLID_FTN_path=IndicLID_FTN_path,
            IndicLID_FTR_path=IndicLID_FTR_path,
            IndicLID_BERT_path=IndicLID_BERT_path,
            input_threshold=input_threshold, roman_lid_threshold=roman_lid_threshold
        )
        self.cld3_detector = gcld3.NNetLanguageIdentifier(min_num_bytes=0, max_num_bytes=1000) # the arguments are probably how much memory to allocate in the gpu. Not sure though!

        self.nllb_detector = fasttext.load_model(nllb_model_path)

        with open(mapping_json_path, "r") as mapping_f:
            self.mapping = json.load(mapping_f) 

    def run_indiclid(self, texts): # Result Examples: [('صباح الخير، الجو جميل اليوم والسماء صافية.', 'snd_Arab', 1.0000496, 'IndicLID-FTN'), ('This text is written in English.', 'eng_Latn', 1.0000498, 'IndicLID-FTR')]
        predictions = self.indiclid.batch_predict(texts, len(texts))
        results = []

        for i in range(len(predictions)):
            results += [
                {
                    "language": self.mapping["indiclid"][predictions[i][1]]["language"],
                    "logits": predictions[i][2],
                    "output-head": predictions[i][3],
                }
            ]

        return results
        
    """ 
        Class Attributes of cld3 results
        self.assertEqual(result.language, "en")
        self.assertTrue(result.is_reliable)
        self.assertGreater(result.proportion, 0.99)
        self.assertGreater(result.probability, 0.90)
    """
    def run_cld3(self, texts): # Result Examples: [<gcld3.pybind_ext.Result object at 0x7f0ca96f51f0>, <gcld3.pybind_ext.Result object at 0x7f0ca80d3570>]
        results = []
        
        for text in texts:
            res = self.cld3_detector.FindLanguage(text=text)
            results += [
                {
                    "language": self.mapping["cld3"][res.language]["language"],
                    "probability": res.probability,
                    "is_reliable": res.is_reliable,
                    "proportion": res.proportion,
                }
            ]

        return results

    def run_nllb(self, texts): # Result Examples: ([['__label__arb_Arab'], ['__label__eng_Latn']], [array([0.99960977], dtype=float32), array([1.0000095], dtype=float32)])
        langs, probs = self.nllb_detector.predict(texts, k=1)
        results = []

        for i in range(len(langs)):
            results += [
                {
                    "language": self.mapping["nllb"][langs[i][0][9:]]["language"],
                    "probability": float(probs[i][0]),
                }
            ]

        return results
    
    def run_single(self, text):
        indiclid_results = self.run_indiclid([text])
        cld3_results = self.run_cld3([text])
        nllb_results = self.run_nllb([text]) 

        majority_lang, language_vote = self.hard_vote(indiclid_res=indiclid_results, cld3_res=cld3_results, nllb_res=nllb_results)
        return majority_lang, language_vote

    def run_batch(self, texts):
        indiclid_results = self.run_indiclid(texts)
        cld3_results = self.run_cld3(texts)
        nllb_results = self.run_nllb(texts) 

        merged_results = list(zip(indiclid_results, cld3_results, nllb_results))
        majority_langs = tuple(map(lambda x: self.hard_vote([x[0]], [x[1]], [x[2]]), merged_results))
        return majority_langs
        
    def hard_vote(self, indiclid_res, cld3_res, nllb_res):
        
        overall_res = indiclid_res + cld3_res + nllb_res
        language_vote = {}
        detector_mapping = {0: "indiclid", 1: "cld3", 2: "nllb"}

        for i in range(len(overall_res)):
            res = overall_res[i]
            if res["language"] not in language_vote.keys():
                language_vote[res["language"]] = {}
            language_vote[res["language"]]["total_votes"] = language_vote[res["language"]].get("total_votes", 0) + 1
            language_vote[res["language"]][detector_mapping[i]] = res["probability" if "probability" in tuple(res.keys()) else "logits"]
        
        lang_vote_pair = list(language_vote.items())
        lang_vote_pair.sort(reverse=True, key=lambda x: x[1]["total_votes"])
        majority_lang = lang_vote_pair[0]
        return majority_lang, language_vote

    def soft_vote(self, indiclid_res, cld3_res, nllb_res):
        pass


if __name__ == "__main__":

    lid = LIDPipeline()
    text = "صباح الخير، الجو جميل اليوم والسماء صافية."
    
    print(lid.run_single(text))

    print("\n\n Batched Results\n")

    texts = [
        "صباح الخير، الجو جميل اليوم والسماء صافية.",
        "This text is written in English.",
    ]
    print(lid.run_batch(texts))
