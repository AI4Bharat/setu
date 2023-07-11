import fasttext
import random
import time



confusion_matrix_mapping  = {
    'Assamese' : 0,
    'Bangla' : 1,
    'Bodo' : 2,
    'Dogri' : 3,
    'Konkani' : 4, 
    'Gujarati' : 5,
    'Hindi' : 6,
    'Kannada' : 7,
    'Kashmiri_Arab' : 8,
    'Kashmiri_Deva' : 9,
    'Maithili' : 10,
    'Malayalam' : 11,
    'Manipuri_Beng' : 12,
    'Manipuri_Mei' : 13,
    'Marathi' : 14,
    'Nepali' : 15,
    'Oriya' : 16,
    'Punjabi' : 17,
    'Sanskrit' : 18,
    'Santali' : 19,
    'Sindhi' : 20,
    'Tamil' : 21,
    'Telugu' : 22,
    'Urdu' : 23,
    'English' : 24,
    'Other' : 25
}

confusion_matrix_reverse_mapping  = {
    0 : 'Assamese',
    1 : 'Bangla',
    2 : 'Bodo',
    3 : 'Dogri',
    4 : 'Konkani', 
    5 : 'Gujarati',
    6 : 'Hindi',
    7 : 'Kannada',
    8 : 'Kashmiri_Arab',
    9 : 'Kashmiri_Deva',
    10 : 'Maithili',
    11 : 'Malayalam',
    12 : 'Manipuri_Beng',
    13 : 'Manipuri_Mei',
    14 : 'Marathi',
    15 : 'Nepali',
    16 : 'Oriya',
    17 : 'Punjabi',
    18 : 'Sanskrit',
    19 : 'Santali',
    20 : 'Sindhi',
    21 : 'Tamil',
    22 : 'Telugu',
    23 : 'Urdu',
    24 : 'English',
    25 : 'Other'
}

def predict(sen, fasttext_model):

    # since we're not training, we don't need to calculate the gradients for our outputs
    # with torch.no_grad():
        # for line in lines_resampled:
            
            # inputs, labels = data[0], data[1]
            # labels = line.split(' ')[0]
            # labels = confusion_matrix_mapping[ labels[9:] ]

    fasttext_prediction = fasttext_model.predict(sen)
    fasttext_pred_label = fasttext_prediction[0][0]
    fasttext_pred_score = fasttext_prediction[1][0]

    # if fasttext_pred_score > threshold:
    return  fasttext_pred_label[9:]
        # else:
        #     word_embeddings = tokenizer(sen, return_tensors="pt", padding=True, truncation=True, max_length=512)   
        #     word_embeddings = word_embeddings.to(device)
        #     # labels = labels.to(device)

        #     outputs = model(word_embeddings['input_ids'], 
        #                 token_type_ids=word_embeddings['token_type_ids'], 
        #                 attention_mask=word_embeddings['attention_mask']
        #                 # ,labels=labels
        #                 )
        #     _, predicted = torch.max(outputs.logits, 1)
            
        #     return confusion_matrix_reverse_mapping[predicted.item()]
            
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










def measure_inferences(lines_in, fasttext_model):
    
    start_time = time.time()
    samples_processed = 0

    while time.time() - start_time < 1:
        # function to process samples here
        samples_processed += 1
        lines_sample = random.sample( lines_in, k=1 )[0]

        inputs = ' '.join(lines_sample.split(' ')[1:])
        # print(predict(line))
        predict(inputs, fasttext_model)

    print("Number of samples processed in 1 second: ", samples_processed)

    return samples_processed

# preparing samples
files_in = open('../corpus/test_combine_native_20.txt', 'r')
lines_in = files_in.read().split('\n')
files_in.close()


file = open('../common_result_1/inferences.txt', 'w')
# fasttext_model = fasttext.load_model('/nlsasfs/home/ai4bharat/yashkm/yash/indic-lid/final_runs_ACL_inference/native_model/fasttext/clean_samples/tune_run/baseline_en_other/result_model_dim_4/model_baseline_roman.bin')
fasttext_model = fasttext.load_model('/nlsasfs/home/ai4bharat/yashkm/yash/indic-lid/final_runs_ACL_inference/native_model/fasttext/clean_samples/tune_run/baseline_en_other/result_model_dim_8/model_baseline_roman.bin')

inferences = measure_inferences(lines_in, fasttext_model)

file.write('Inferences - model dim : ' + str(inferences) + '\n')
# print('Inference time for threshold ' + str(threshold) + ' : ' + str(average_inference_time) + '\n')

file.close()




    


# evaluate( 'test_combine', model)
# evaluate( 'test_dakshina_original_roman', model)
# evaluate( 'valid_dakshina_original_roman', model)
# evaluate( 'test_dakshina_filter_roman', model)
# evaluate( 'test_dakshina_indicxlit_romanized', model)
# evaluate( 'test_combine_flores200_romanized', model)
# evaluate( 'test_combine_ai4b_romanized', model)




