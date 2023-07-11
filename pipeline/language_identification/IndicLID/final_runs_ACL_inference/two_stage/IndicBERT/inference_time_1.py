import fasttext
fasttext_lid_model_path='/nlsasfs/home/ai4bharat/yashkm/yash/indic-lid/final_runs/roman_model/fasttext/clean_samples/tune_run/basline_en_other/result_model_dim_8/model_baseline_roman.bin'

fasttext_model = fasttext.load_model(fasttext_lid_model_path)

import time
from tqdm import tqdm
import pandas as pd
import csv
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset
from torch.utils.data import DataLoader
import numpy as np
import torch.optim as optim
from transformers import AutoModelForSequenceClassification
from transformers import AutoTokenizer
import random
from transformers import AutoModel, AutoTokenizer
import transformers
import sys

device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')

IndicBERT_lid_model_path = '/nlsasfs/home/ai4bharat/yashkm/yash/indic-lid/final_runs/roman_model/finetuning/clean_samples/IndicBERT/unfreeze_layers/result_unfreeze_1/basline_nn_simple.pt'
model = torch.load(IndicBERT_lid_model_path, map_location=device)
model.eval()

tokenizer = AutoTokenizer.from_pretrained("ai4bharat/IndicBERTv2-MLM-only")

confusion_matrix_mapping  = {
    'Assamese' : 0,
    'Bangla' : 1,
    'Bodo' : 2,
    'Konkani' : 3, 
    'Gujarati' : 4,
    'Hindi' : 5,
    'Kannada' : 6,
    'Kashmiri' : 7,
    'Maithili' : 8,
    'Malayalam' : 9,
    'Manipuri_Mei' : 10,
    'Marathi' : 11,
    'Nepali' : 12,
    'Oriya' : 13,
    'Punjabi' : 14,
    'Sanskrit' : 15,
    'Sindhi' : 16,
    'Tamil' : 17,
    'Telugu' : 18,
    'Urdu' : 19,
    'English' : 20,
    'Other' : 21
}

confusion_matrix_reverse_mapping  = {
    0 : 'Assamese',
    1 : 'Bangla',
    2 : 'Bodo',
    3 : 'Konkani', 
    4 : 'Gujarati',
    5 : 'Hindi',
    6 : 'Kannada',
    7 : 'Kashmiri',
    8 : 'Maithili',
    9 : 'Malayalam',
    10 : 'Manipuri_Mei',
    11 : 'Marathi',
    12 : 'Nepali',
    13 : 'Oriya',
    14 : 'Punjabi',
    15 : 'Sanskrit',
    16 : 'Sindhi',
    17 : 'Tamil',
    18 : 'Telugu',
    19 : 'Urdu',
    20 : 'English',
    21 : 'Other'
}

def predict(sen, threshold):

    # since we're not training, we don't need to calculate the gradients for our outputs
    with torch.no_grad():
        # for line in lines_resampled:
            
            # inputs, labels = data[0], data[1]
            # labels = line.split(' ')[0]
            # labels = confusion_matrix_mapping[ labels[9:] ]

        fasttext_prediction = fasttext_model.predict(sen)
        fasttext_pred_label = fasttext_prediction[0][0]
        fasttext_pred_score = fasttext_prediction[1][0]

        if fasttext_pred_score > threshold:
            return  fasttext_pred_label[9:]
        else:
            word_embeddings = tokenizer(sen, return_tensors="pt", padding=True, truncation=True, max_length=512)   
            word_embeddings = word_embeddings.to(device)
            # labels = labels.to(device)

            outputs = model(word_embeddings['input_ids'], 
                        token_type_ids=word_embeddings['token_type_ids'], 
                        attention_mask=word_embeddings['attention_mask']
                        # ,labels=labels
                        )
            _, predicted = torch.max(outputs.logits, 1)
            
            return confusion_matrix_reverse_mapping[predicted.item()]
            
                # print(outputs.logits)
                # print(predicted)
            
            # for sen, label, pred_label, logit in zip(inputs, labels, predicted, outputs.logits):
                
            #     fasttext_prediction = fasttext_model.predict(sen)
            #     fasttext_pred_label = fasttext_prediction[0][0]
            #     fasttext_pred_score = fasttext_prediction[1][0]
                
            #     pred_score = logit[pred_label.item()].item()
                
            #     if fasttext_pred_score > threshold:
            #         if confusion_matrix_mapping[fasttext_pred_label[9:]] == label.item():
            #             final_count+=1
            #     else:
            #         if label.item() == pred_label.item():
            #             final_count+=1
            #         else:             

            #     final_n+=1

            # total += labels.size(0)
            # correct += (predicted == labels).sum().item()

    #     acc = ((100 * correct) / total)

    # final_acc = ((100 * final_count) / final_n)
    # print(f'Accuracy of the network on the {total} test inputs: {final_acc} ')   
    # file.write('Test Set ('+file_name+') : ' + str(final_acc) + '\n')








def measure_inferences(lines_in, threshold):
    
    start_time = time.time()
    samples_processed = 0

    while time.time() - start_time < 1:
        # function to process samples here
        samples_processed += 1
        lines_sample = random.sample( lines_in, k=1 )[0]

        inputs = ' '.join(lines_sample.split(' ')[1:])
        # print(predict(line))
        predict(inputs, threshold)

    print("Number of samples processed in 1 second: ", samples_processed)

    return samples_processed

# preparing samples
files_in = open('../corpus/test_combine_roman.txt', 'r')
lines_in = files_in.read().split('\n')
files_in.close()


file = open('../common_result_1/inference_time.txt', 'w')
thresholds = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
for threshold in thresholds:
    avg_list = []
    for _ in range(10):
        inferences = measure_inferences(lines_in, threshold)
        avg_list.append(inferences)
    
    avg_inferences = sum(avg_list)/10
    file.write('Inferences for threshold ' + str(threshold) + ' : ' + str(avg_inferences) + '\n')

file.close()




    


# evaluate( 'test_combine', model)
# evaluate( 'test_dakshina_original_roman', model)
# evaluate( 'valid_dakshina_original_roman', model)
# evaluate( 'test_dakshina_filter_roman', model)
# evaluate( 'test_dakshina_indicxlit_romanized', model)
# evaluate( 'test_combine_flores200_romanized', model)
# evaluate( 'test_combine_ai4b_romanized', model)



