import fasttext
import json
import glob
from pyspark.sql.types import (
    MapType, 
    StringType, 
    FloatType,
    StructType, 
    StructField
)
import gcld3
import os
import sys
import re
from tqdm import tqdm
import pandas as pd
import numpy as np
import csv
import random
import subprocess
from pyspark.sql.functions import col

import fasttext

import torch
import torch.nn as nn
from torch.utils.data import Dataset
from transformers import AutoTokenizer
from functools import partial
import gc
from base import SetuStage
from utilities import str2bool,list_of_strings


class IndicBERT_Data(Dataset):
    def __init__(self, indices: list, X: list) -> None:
        """
        Dataset for IndicBERT.

        Args:
            indices (List[int]): List of indices.
            X (List[str]): List of texts.

        Returns:
            None
        """
        self.size = len(X)
        """Size of the text dataset."""
        self.x = X
        """A list of text strings."""
        self.i = indices
        """A list of index values."""

    def __len__(self) -> int:
        """
        Get the length of the dataset.

        Returns:
            int: The length of the dataset.
        """
        return self.size

    def __getitem__(self, idx: int) -> tuple:
        """
        Get an item from the dataset by index.

        Args:
            idx (int): Index of the item to retrieve.

        Returns:
            Tuple[int, str]: A tuple containing the index and text.
        """
        text = self.x[idx]
        index = self.i[idx]
        return tuple([index, text])


class IndicLID():
    """IndicLID Main class representation for the IndicLID Model.
    """
    def __init__(self, 
                 indiclid_ftn_path:str = 'models/indiclid-ftn/model_baseline_roman.bin',
                 indiclid_ftr_path:str = 'models/indiclid-ftr/model_baseline_roman.bin',
                 indiclid_bert_path:str = 'models/indiclid-bert/basline_nn_simple.pt',
                 input_threshold:float = 0.5, roman_lid_threshold:float = 0.6):
        """__init__ Method to initialize the IndicLID Class.

        Args:
            indiclid_ftn_path (str, optional): Path to the Indic Native Model. Defaults to 'models/indiclid-ftn/model_baseline_roman.bin'.
            indiclid_ftr_path (str, optional): Path to the Indic Roman Model. Defaults to 'models/indiclid-ftr/model_baseline_roman.bin'.
            indiclid_bert_path (str, optional): Path to the Indic Bert based Model. Defaults to 'models/indiclid-bert/basline_nn_simple.pt'.
            input_threshold (float, optional): Threshold for difference between native and roman script. Defaults to 0.5.
            roman_lid_threshold (float, optional): Threshold for the roman LID model. Defaults to 0.6.
        """
        # define dictionary for roman and native languages to langauge code
        # define input_threhsold percentage for native and roman script input diversion 
        # define model_threhsold for roman script model

        # self.device = torch.device('cuda:1' if torch.cuda.is_available() else 'cpu')
        self.device = torch.device('cpu')
        """@private"""

        self.indiclid_ftn_path = indiclid_ftn_path
        """@private"""
        self.indiclid_ftr_path = indiclid_ftr_path
        """@private"""
        self.indiclid_bert_path = indiclid_bert_path
        """@private"""

        self.IndicLID_FTN = fasttext.load_model(self.indiclid_ftn_path)
        """@private"""
        self.IndicLID_FTR = fasttext.load_model(self.indiclid_ftr_path)
        """@private"""
        self.IndicLID_BERT = torch.load(self.indiclid_bert_path, map_location = self.device)
        """@private"""
        self.IndicLID_BERT.eval()
        self.IndicLID_BERT_tokenizer = AutoTokenizer.from_pretrained("ai4bharat/IndicBERTv2-MLM-only", cache_dir=".hf_cache/tokenizer")
        """@private"""
            
        self.input_threshold = input_threshold
        """@private"""
        self.model_threshold = roman_lid_threshold
        """@private"""
        self.classes = 47
        """@private"""     
        
        self.IndicLID_lang_code_dict = {
            'asm_Latn' : 0,
            'ben_Latn' : 1,
            'brx_Latn' : 2,
            'guj_Latn' : 3,
            'hin_Latn' : 4,
            'kan_Latn' : 5,
            'kas_Latn' : 6,
            'kok_Latn' : 7,
            'mai_Latn' : 8,
            'mal_Latn' : 9,
            'mni_Latn' : 10,
            'mar_Latn' : 11,
            'nep_Latn' : 12,
            'ori_Latn' : 13,
            'pan_Latn' : 14,
            'san_Latn' : 15,
            'snd_Latn' : 16,
            'tam_Latn' : 17,
            'tel_Latn' : 18,
            'urd_Latn' : 19,
            'eng_Latn' : 20,
            'other' : 21,
            'asm_Beng' : 22,
            'ben_Beng' : 23,
            'brx_Deva' : 24,
            'doi_Deva' : 25,
            'guj_Gujr' : 26,
            'hin_Deva' : 27,
            'kan_Knda' : 28,
            'kas_Arab' : 29,
            'kas_Deva' : 30,
            'kok_Deva' : 31,
            'mai_Deva' : 32,
            'mal_Mlym' : 33,
            'mni_Beng' : 34,
            'mni_Meti' : 35,
            'mar_Deva' : 36,
            'nep_Deva' : 37,
            'ori_Orya' : 38,
            'pan_Guru' : 39,
            'san_Deva' : 40,
            'sat_Olch' : 41,
            'snd_Arab' : 42,
            'tam_Tamil' : 43,
            'tel_Telu' : 44,
            'urd_Arab' : 45
        }
        """@private"""



        self.IndicLID_lang_code_dict_reverse = {
            0 : 'asm_Latn',
            1 : 'ben_Latn',
            2 : 'brx_Latn',
            3 : 'guj_Latn',
            4 : 'hin_Latn',
            5 : 'kan_Latn',
            6 : 'kas_Latn',
            7 : 'kok_Latn',
            8 : 'mai_Latn',
            9 : 'mal_Latn',
            10 : 'mni_Latn',
            11 : 'mar_Latn',
            12 : 'nep_Latn',
            13 : 'ori_Latn',
            14 : 'pan_Latn',
            15 : 'san_Latn',
            16 : 'snd_Latn',
            17 : 'tam_Latn',
            18 : 'tel_Latn',
            19 : 'urd_Latn',
            20 : 'eng_Latn',
            21 : 'other',
            22 : 'asm_Beng',
            23 : 'ben_Beng',
            24 : 'brx_Deva',
            25 : 'doi_Deva',
            26 : 'guj_Gujr',
            27 : 'hin_Deva',
            28 : 'kan_Knda',
            29 : 'kas_Arab',
            30 : 'kas_Deva',
            31 : 'kok_Deva',
            32 : 'mai_Deva',
            33 : 'mal_Mlym',
            34 : 'mni_Beng',
            35 : 'mni_Meti',
            36 : 'mar_Deva',
            37 : 'nep_Deva',
            38 : 'ori_Orya',
            39 : 'pan_Guru',
            40 : 'san_Deva',
            41 : 'sat_Olch',
            42 : 'snd_Arab',
            43 : 'tam_Tamil',
            44 : 'tel_Telu',
            45 : 'urd_Arab'
        }
        """@private"""

    def pre_process(self, input:str)->str:
        """pre_process pre-process the input in the same way as we pro-process the training sample

        Args:
            input (str): The input text string.

        Returns:
            str: The output text string.
        """
        return input


    def char_percent_check(self, input:str)->float:
        """char_percent_check Checks whether input has input_threhsold of roman characters

        Args:
            input (str): The input text string.

        Returns:
            float: Value indicating the percentage of roman characters.
        """
        
        # count total number of characters in string
        input_len = len(list(input))

        # count special character spaces and newline in string
        special_char_pattern = re.compile('[@_!#$%^&*()<>?/\|}{~:]')
        special_char_matches = special_char_pattern.findall(input)
        special_chars = len(special_char_matches)
        
        spaces = len(re.findall('\s', input))
        newlines = len(re.findall('\n', input))

        # subtract total-special character counts
        total_chars = input_len - (special_chars + spaces + newlines)

        # count the number of english character and digit in string
        en_pattern = re.compile('[a-zA-Z0-9]')
        en_matches = en_pattern.findall(input)
        en_chars = len(en_matches)

        # calculate the percentage of english character in total number of characters
        if total_chars == 0:
            return 0
        return (en_chars/total_chars)



    def native_inference(self, input_list:list, output_dict:dict)->dict:
        """native_inference Method that performance Indic Native inference on the input list of strings.

        Args:
            input_list (list): A list of text strings.
            output_dict (dict): A dictionary containing the input string followed by label, score and model type.

        Returns:
            dict: A dictionary containing the input string followed by label, score and model type.
        """

        if not input_list:
            return output_dict
        
        # inference for fasttext native script model
        input_texts = [line[1] for line in input_list]
        IndicLID_FTN_predictions = self.IndicLID_FTN.predict(input_texts)
        
        # add result of input directly to output_dict
        for input, pred_label, pred_score in zip(input_list, IndicLID_FTN_predictions[0], IndicLID_FTN_predictions[1]):
            # print(pred_score)
            output_dict[input[0]] = (input[1], pred_label[0][9:], pred_score[0], 'IndicLID-FTN')

        return output_dict

    def roman_inference(self, input_list:list, output_dict:dict, batch_size:int)->dict:
        """roman_inference Method that performance Indic Roman inference on the input list of strings.

        Args:
            input_list (list): A list of text strings.
            output_dict (dict): A dictionary containing the input string followed by label, score and model type.
            batch_size (int): Value indicating the number of samples in the input batch during prediction.

        Returns:
            dict: A dictionary containing the input string followed by label, score and model type.
        """
        if not input_list:
            return output_dict
        
        # 1st stage
        # inference for fasttext roman script model
        input_texts = [line[1] for line in input_list]
        IndicLID_FTR_predictions = self.IndicLID_FTR.predict(input_texts)
        
        IndicLID_BERT_inputs = []
        # add result of input directly to output_dict
        for input, pred_label, pred_score in zip(input_list, IndicLID_FTR_predictions[0], IndicLID_FTR_predictions[1]):
            if pred_score[0] > self.model_threshold:
                output_dict[input[0]] = (input[1], pred_label[0][9:], pred_score[0], 'IndicLID-FTR')
            else:
                IndicLID_BERT_inputs.append(input)
        
        # 2nd stage
        output_dict = self.IndicBERT_roman_inference(IndicLID_BERT_inputs, output_dict, batch_size)
        return output_dict

    
    def IndicBERT_roman_inference(self, IndicLID_BERT_inputs:list, output_dict:dict, batch_size:int)->dict:
        # inference for IndicBERT roman script model
        """IndicBERT_roman_inference Method that performance Indic Roman inference on the input list of strings.

        Args:
            input_list (list): A list of text strings.
            output_dict (dict): A dictionary containing the input string followed by label, score and model type.
            batch_size (int): Value indicating the number of samples in the input batch during prediction.

        Returns:
            dict: A dictionary containing the input string followed by label, score and model type.
        """
        if not IndicLID_BERT_inputs:
            return output_dict
        
        df = pd.DataFrame(IndicLID_BERT_inputs)
        dataloader = self.get_dataloaders(df.iloc[:,0], df.iloc[:,1], batch_size)


        with torch.no_grad():
            for data in dataloader:
                batch_indices = data[0]
                batch_inputs = data[1]

                word_embeddings = self.IndicLID_BERT_tokenizer(batch_inputs, return_tensors="pt", padding=True, truncation=True, max_length=512)
                    
                word_embeddings = word_embeddings.to(self.device)
            
                batch_outputs = self.IndicLID_BERT(word_embeddings['input_ids'], 
                            token_type_ids=word_embeddings['token_type_ids'], 
                            attention_mask=word_embeddings['attention_mask']
                            )
                

                _, batch_predicted = torch.max(batch_outputs.logits, 1)
            
            
                for index, input, pred_label, logit in zip(batch_indices, batch_inputs, batch_predicted, batch_outputs.logits):
                    output_dict[index] = (input,
                                            self.IndicLID_lang_code_dict_reverse[pred_label.item()], 
                                            logit[pred_label.item()].item(), 'IndicLID-BERT'
                                            )


        return output_dict


    def post_process(self, output_dict:dict)->list:
        """post_process Post-process the outputs from the inference stage.

        Args:
            output_dict (dict): A dictionary containing the input string followed by label, score and model type.

        Returns:
            list: The output text string.
        """
        # output the result in some consistent language code format
        results = []
        keys = list(output_dict.keys())
        keys.sort()
        for index in keys:
            results.append( output_dict[index] )

        return results
    
    def get_dataloaders(self, indices, input_texts, batch_size):
        data_obj = IndicBERT_Data(indices, input_texts)
        dl = torch.utils.data.DataLoader(data_obj,
                                                    batch_size=batch_size,
                                                    shuffle=False
                                                )
        return dl
        
    def predict(self, input):
        input_list = [input,]
        self.batch_predict(input_list, 1)

    def batch_predict(self, input_list, batch_size):

        # call functions seq by seq and divert the input to IndicBERT if 
        # fasttext prediction score is less than the defined model_threhsold.
        # Also output the inference time along with the result.
        output_dict = {}

        roman_inputs = []
        native_inputs = []

        # text roman percent check 
        for index, input in enumerate(input_list):
            if self.char_percent_check(input) > self.input_threshold:
                roman_inputs.append((index, input))
            else:
                native_inputs.append((index, input))
        
        output_dict = self.native_inference(native_inputs, output_dict)
        output_dict = self.roman_inference(roman_inputs, output_dict, batch_size)
        
        results = self.post_process(output_dict)
        return results


class LIDPipeline():

    def __init__(
        self,
        indiclid_ftn_path = 'data/models/indiclid-ftn/model_baseline_roman.bin',
        indiclid_ftr_path = 'data/models/indiclid-ftr/model_baseline_roman.bin',
        indiclid_bert_path = 'data/models/indiclid-bert/basline_nn_simple.pt',
        input_threshold = 0.5,
        roman_lid_threshold = 0.6, 
        nllb_model_path="data/models/lid218e.bin",
        mapping_json_path="data/language_mapping.json",
        iso_mapping_json_path="data/lang_iso_mapping.json",
        lid_probability_threshold=0.7,
        **kwargs,
    ):
        
        self.indiclid = IndicLID(
            indiclid_ftn_path=indiclid_ftn_path,
            indiclid_ftr_path=indiclid_ftr_path,
            indiclid_bert_path=indiclid_bert_path,
            input_threshold=input_threshold, 
            roman_lid_threshold=roman_lid_threshold
        )
        self.cld3_detector = gcld3.NNetLanguageIdentifier(min_num_bytes=0, max_num_bytes=1000) # the arguments are probably how much memory to allocate in the gpu. Not sure though!

        self.nllb_detector = fasttext.load_model(nllb_model_path)

        with open(mapping_json_path, "r") as mapping_f:
            self.mapping = json.load(mapping_f) 

        with open(iso_mapping_json_path, "r") as lang_website_f:
            self.language_iso_mapping = json.load(lang_website_f)

        self.lid_probability_threshold = lid_probability_threshold

    def run_indiclid(self, texts): # Result Examples: [('صباح الخير، الجو جميل اليوم والسماء صافية.', 'snd_Arab', 1.0000496, 'IndicLID-FTN'), ('This text is written in English.', 'eng_Latn', 1.0000498, 'IndicLID-FTR')]
        predictions = self.indiclid.batch_predict(texts, len(texts))
        results = []

        for i in range(len(predictions)):
            results += [
                {
                    "language": self.mapping["indiclid"][predictions[i][1]]["language"],
                    "script": self.mapping["indiclid"][predictions[i][1]]["script"],
                    "code": predictions[i][1],
                    "logits": float(predictions[i][2]) ,
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
                    "language": self.mapping["cld3"].get(res.language, {"language": "other"})["language"],
                }
            ]
            results[-1]["probability"] = res.probability if results[-1]["language"] != "other" else 0.0
            results[-1]["is_reliable"] = res.is_reliable if results[-1]["language"] != "other" else False
            results[-1]["proportion"] = res.proportion if results[-1]["language"] != "other" else 0.0

        return results

    def run_nllb(self, texts): # Result Examples: ([['__label__arb_Arab'], ['__label__eng_Latn']], [array([0.99960977], dtype=float32), array([1.0000095], dtype=float32)])
        langs, probs = self.nllb_detector.predict(texts, k=1)
        results = []

        for i in range(len(langs)):
            results += [
                {
                    "language": self.mapping["nllb"].get(langs[i][0][9:], {"language": "other"})["language"],
                }
            ]
            results[-1]["probability"] = float(probs[i][0]) if results[-1]["language"] != "other" else 0.0

        return results
    
    def run_lid_single(self, text, for_spark=True, lid_probability_threshold=None):
        indiclid_results = self.run_indiclid([text])
        cld3_results = self.run_cld3([text])
        nllb_results = self.run_nllb([text]) 

        majority_lang, language_vote = self.hard_vote(
            indiclid_res=indiclid_results, 
            cld3_res=cld3_results, 
            nllb_res=nllb_results,
            lid_probability_threshold=lid_probability_threshold,
        )

        lang_per_model = {
            "indiclid_lang": indiclid_results[-1]["language"],
            "nllb_lang": nllb_results[-1]["language"],
            "cld3_lang": cld3_results[-1]["language"],
        }

        out_per_model = {
            "indiclid_logit": indiclid_results[-1]["logits"],
            "nllb_probability": nllb_results[-1]["probability"],
            "cld3_probability": cld3_results[-1]["probability"],
        }
        
        
        indiclid_code = indiclid_results[-1]["code"]
        indiclid_script = indiclid_results[-1]["script"]

        if for_spark:
            return majority_lang, lang_per_model, out_per_model, indiclid_code, indiclid_script

        return majority_lang, language_vote, lang_per_model, out_per_model, indiclid_code, indiclid_script
        
    def hard_vote(self, indiclid_res, cld3_res, nllb_res, lid_probability_threshold=None):

        if lid_probability_threshold:
           self.lid_probability_threshold = lid_probability_threshold
        
        overall_res = indiclid_res + cld3_res + nllb_res
        language_vote = {}
        detector_mapping = {0: "indiclid", 1: "cld3", 2: "nllb"}

        for i in range(len(overall_res)):
            res = overall_res[i]
            if res["language"] not in language_vote.keys():
                language_vote[res["language"]] = {}

            language_vote[res["language"]][detector_mapping[i]] = res["probability" if "probability" in tuple(res.keys()) else "logits"]

            # if probability is there (like cld3 & NLLB), then it should be above the threshold to be considered, 
            # else if probability is not present at all (like IndicLID), then we will always consider it for voting.
            if "probability" in tuple(res.keys()) and res["probability"] >= self.lid_probability_threshold: 
                language_vote[res["language"]]["total_votes"] = language_vote[res["language"]].get("total_votes", 0) + 1
            else:
                language_vote[res["language"]]["total_votes"] = language_vote[res["language"]].get("total_votes", 0) + 1
        
        lang_vote_pair = list(language_vote.items())
        lang_vote_pair.sort(reverse=True, key=lambda x: x[1]["total_votes"])
        majority_lang = lang_vote_pair[0]
        return majority_lang, language_vote

    def get_iso_code(self, lang):
        return self.language_iso_mapping[lang]
    
def run_lid_on_each_partition_with_idx(
    idx, 
    partition, 
    identifier_cols, 
    text_col,
    indiclid_ftn_path,
    indiclid_ftr_path,
    indiclid_bert_path,
    input_threshold,
    roman_lid_threshold,
    nllb_model_path,
    mapping_json_path,
    iso_mapping_json_path,
    lid_probability_threshold,
):

    langs = [
        "assamese",
        "bengali",
        "bodo",
        "dogri",
        "english",
        "gujarati",
        "hindi",
        "kannada",
        "kashmiri",
        "konkani",
        "maithili",
        "malayalam",
        "manipuri",
        "marathi",
        "nepali",
        "oriya",
        "punjabi",
        "sanskrit",
        "santhali",
        "sindhi",
        "tamil",
        "telugu",
        "urdu",
        "other",
    ]

    print(f"Performing LID on partition {idx}......")

    lid = LIDPipeline(
        indiclid_ftn_path=indiclid_ftn_path,
        indiclid_ftr_path=indiclid_ftr_path,
        indiclid_bert_path=indiclid_bert_path,
        input_threshold=input_threshold,
        roman_lid_threshold=roman_lid_threshold, 
        nllb_model_path=nllb_model_path,
        mapping_json_path=mapping_json_path,
        iso_mapping_json_path=iso_mapping_json_path,
        lid_probability_threshold=lid_probability_threshold
    )

    print(f"Loaded LID for partition {idx}...... Starting LID")

    for row in partition:
        majority_lang, lang_per_model, out_per_model, indiclid_code, indiclid_script = lid.run_lid_single(row[text_col].replace('\n', ' '), for_spark=True)
        majority_lang = majority_lang[0]
        if majority_lang not in langs:
            majority_lang = "other"
            iso_code = "other"
        else:
            iso_code = lid.get_iso_code(majority_lang)
        res_list = [row[id_col] for id_col in identifier_cols] + [majority_lang, iso_code, lang_per_model, out_per_model, indiclid_code, indiclid_script]
        yield res_list

    del lid
    gc.collect()
    torch.cuda.empty_cache()


def run_lid_spark_pipeline(spark, config, df, identifier_cols, text_col, lang_col_name, iso_col_name, data_root):

    run_lid = partial(
        run_lid_on_each_partition_with_idx,
        identifier_cols=identifier_cols, 
        text_col=text_col,
        indiclid_ftn_path=os.path.join(data_root, config.indiclid_ftn_path),
        indiclid_ftr_path=os.path.join(data_root, config.indiclid_ftr_path),
        indiclid_bert_path=os.path.join(data_root, config.indiclid_bert_path),
        input_threshold=config.input_threshold,
        roman_lid_threshold=config.roman_lid_threshold,
        nllb_model_path=os.path.join(data_root, config.nllb_model_path),
        mapping_json_path=os.path.join(data_root, config.mapping_json_path),
        iso_mapping_json_path=os.path.join(data_root, config.iso_mapping_json_path),
        lid_probability_threshold=config.lid_probability_threshold,
    )

    result_schema = StructType([
            StructField(id_col, StringType(), True)
                for id_col in identifier_cols
        ] + [
            StructField(lang_col_name, StringType(), True), 
            StructField(iso_col_name, StringType(), True),
            StructField("lang_per_model", MapType(StringType(), StringType()), True),
            StructField("out_per_model", MapType(StringType(), FloatType()), True), 
            StructField("indiclid_code", StringType(), True), 
            StructField("indiclid_script", StringType(), True),
        ])

    lang_rdd = df \
            .select(*identifier_cols, text_col) \
            .rdd.mapPartitionsWithIndex(run_lid)

    lang_df = spark.createDataFrame(lang_rdd, schema=result_schema)

    cols_list = identifier_cols + [lang_col_name, iso_col_name, "indiclid_code", "indiclid_script"]

    lang_df = lang_df.select(
        *cols_list,
        *[lang_df.lang_per_model[i].alias(i) 
                for i in ["indiclid_lang", "nllb_lang", "cld3_lang"]],
        *[lang_df.out_per_model[i].alias(i) 
                for i in ["indiclid_logit", "nllb_probability", "cld3_probability"]],
    )
        
    df = df.join(lang_df, identifier_cols)
    
    return df

class LIDStage(SetuStage):

    def __init__(self, config):
        super().__init__(config)

    @staticmethod
    def add_cmdline_args(parser):

        parser.add_argument(
            "--lid_df_parquets_path",
            type=str,
            help="Path to folder containing parquets",
        )

        parser.add_argument(
            "--is_lid_df_path_batched",
            type=str2bool,
            required=False,
            default=True,
            help="Is path a batch path or not?",
        )

        parser.add_argument(
            "--lid_additional_cols",
            type=list_of_strings,
            help="`,` separated additional columns to select from parquets",
        )

        parser.add_argument(
            "--lid_samples_per_partition",
            type=int,
            default=1500,
            required=False,
            help="No.of samples per partition",
        )

        parser.add_argument(
            "--lid_verbose",
            type=str2bool,
            default=False,
            required=False,
            help="Whether to add `show()`",
        )

        parser.add_argument(
            "--lid_run_mode",
            type=str,
            required=False,
            choices=["data"],
            default="data",
            help="Which mode to run the stage in.",
        )

        parser.add_argument(
            "--doc_lid_output_path",
            type=str,
            help="Path of the folder store lid checkpoint",
        )
        
        return parser

    def run_stage_parallelized(
        self,
        df,
        doc_id_col,
        text_col,
        additional_cols_to_use,
        docs_per_partition,
        doc_lid_output_path,
        verbose: bool = True,
        **kwargs
    ):
        raise NotImplementedError("`run_stage_parallelized` function has not been implemented for class `LIDStage`")
    
    def run_data_parallelized(
        self,
        spark,
        df,
        doc_id_col,
        text_col,
        additional_cols_to_use,
        docs_per_partition,
        doc_lid_output_path,
        verbose: bool = True,
        **kwargs
    ):
        print("Starting SETU LID Segregation Spark Pipeline...........")

        print(f"Input document count: {df.count()}")

        df = df.na.drop(subset=[text_col])

        self.df_total_rows = df.count()

        print(f"Count after filtering for `None` values {text_col} col: {self.df_total_rows}")

        df = df.select(doc_id_col, text_col, *additional_cols_to_use)
        df = self.set_split_count_and_salt(df, docs_per_partition)
        df.cache()

        df = run_lid_spark_pipeline(spark, self.config, df, [doc_id_col] + additional_cols_to_use, text_col, "doc_lang", "doc_lang_iso", "/opt/setu/filter_data")
        
        if verbose:
            df.show(n=5)
            print("Completed `doc_lang`....")

        df = self.salting(df, self.n_splits)

        # Duplicate the doc_lang column as doc_lang_partition
        df = df.withColumn("doc_lang_partition", col("doc_lang"))

        df.write.partitionBy("doc_lang_partition").mode("overwrite") \
            .parquet(doc_lid_output_path)

        # rename_partitioned_directories(doc_lid_output_path, "doc_lang_partition")

        print(f"Completed `doc_lang` level `df` parquet write.... to: {doc_lid_output_path}")

    def run_spark(
        self,
        spark,
        lid_df_parquets_path,
        is_lid_df_path_batched,
        lid_additional_cols,
        lid_samples_per_partition,
        lid_verbose,
        lid_run_mode,
        doc_lid_output_path,
        run_local,
        **kwargs
    ):

        if is_lid_df_path_batched:
            if not run_local:
                subprocess.run([[
                    "gsutil",
                    "cp",
                    lid_df_parquets_path,
                    "/tmp/lid_batch.info"
                ]])
            with open("/tmp/lid_batch.info", "r") as batch_f:
                parquet_list = [line.strip() for line in batch_f.readlines()]
            doc_df = spark.read.format("parquet").load(parquet_list)
        else:
            doc_df = spark.read.format("parquet").load(lid_df_parquets_path)

        if lid_run_mode == "stage":
            return self.run_stage_parallelized(
                df=doc_df,
                doc_id_col="doc_id",
                text_col="text",
                additional_cols_to_use=lid_additional_cols,
                docs_per_partition=lid_samples_per_partition,
                doc_lid_output_path=doc_lid_output_path,
                verbose=lid_verbose,
            )
        elif lid_run_mode == "data":
            return self.run_data_parallelized(
                spark=spark,
                df=doc_df,
                doc_id_col="doc_id",
                text_col="text",
                additional_cols_to_use=lid_additional_cols,
                docs_per_partition=lid_samples_per_partition,
                doc_lid_output_path=doc_lid_output_path,
                verbose=lid_verbose,
            )
        else:
            raise Exception("Incorrect input for `lid_run_mode`. `lid_run_mode` only supports 2 types: `stage` & `data`.")

    def run(self, spark, **kwargs):
        if self.config.use_spark and self.spark_present:
            return self.run_spark(spark, **kwargs)
        else:
            return self.run_normal(**kwargs)

    def run_normal(
        self,
        lid_df_parquets_path,
        is_lid_df_path_batched,
        lid_additional_cols,
        lid_samples_per_partition,
        lid_verbose,
        lid_run_mode,
        doc_lid_output_path,
        run_local,
    ):
        raise NotImplementedError("`run_normal` function has not been implemented for class `LIDStage`")
