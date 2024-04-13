import os
import math
import numpy as np
from shapely import Polygon
import pandas as pd
from functools import partial
import builtins as py_builtin
from pyspark.sql.types import Row
from typing import Iterable

def get_max_lang(lang_scores:dict)->Row:
    """get_max_lang Function that fetches the code and confidence score given a dictionary of GCP OCR LID Scores.

    Args:
        lang_scores (dict): Dictionary containing language scores from GCP OCR Output.

    Returns:
        Row: A Spark SQL Row containing the max scored language's code and its corresponding score.
    """
    max_conf = 0.0
    max_lang = None
    if lang_scores:
        for lang_score in lang_scores:
            if lang_score["confidence"] > max_conf:
                max_conf = lang_score["confidence"]
                max_lang = lang_score["languageCode"]
    return Row("languageCode", "lang_confidence")(max_lang, max_conf)

def approx_intersection_suppression(boxes:Iterable, scores:Iterable, threshold:float)->list:
    """approx_intersection_suppression Function that computes intersection among the list of boxes and retains boxes that intersect under the threshold.

    Args:
        boxes (Iterable): The list of GCP bounding boxes.
        scores (Iterable): The list of intersection scores among the boxes.
        threshold (float): Intersection threshold above which the boxes intersect should be removed.

    Returns:
        list: List of boxes retained after filtering based on intersection threshold.
    """
    # Sort the boxes by score in descending order
    order = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)
    keep = []
    while order:
        i = order.pop(0)
        keep.append(i)
        for j in order:
            if ( boxes[i][0] <= boxes[j][0] and boxes[i][1] <= boxes[j][1] ) and ( boxes[i][2] >= boxes[j][2] and boxes[i][3] >= boxes[j][3] ):
                order.remove(j)
            else:
                x_dist = py_builtin.min(boxes[i][2], boxes[j][2]) - py_builtin.max(boxes[i][0], boxes[j][0])
                y_dist = py_builtin.min(boxes[i][3], boxes[j][3]) - py_builtin.max(boxes[i][1], boxes[j][1])
                if x_dist > 0 and y_dist > 0:
                    areaI = x_dist * y_dist
                    area_of_smaller = scores[j] 
                    if not area_of_smaller:
                        order.remove(j)
                    elif areaI/area_of_smaller > threshold:
                        order.remove(j)
    return keep

def approx_intersection_cleaner(bboxes:Iterable, for_spark=True)->Row:
    """approx_intersection_cleaner Function to perform apporx intersection suppression and return bounding boxes to keep.

    Args:
        bboxes (Iterable): List of bounding boxes.
        for_spark (bool, optional): Whether to return result for spark or normal execution. Defaults to True.

    Returns:
        Row: _description_
    """
    # Looping through the block list to get all the paragraph-level bounding boxes
    paragraph_bboxes = []
    for block in bboxes:
        for paragraph in block["paragraphs"]:
            para_bbox = []
            for coords in paragraph["boundingBox"]["normalizedVertices"]:
                x, y = 0.0, 0.0
                if coords["x"]:
                    x = coords["x"]
                if coords["y"]:
                    y = coords["y"]
                para_bbox += [(x, y)]
            paragraph_bboxes += [para_bbox]
    
    para_bboxes_array = np.array(paragraph_bboxes)

    # Using `approx_intersection_suppression` to remove EXTREMELY overlapping bounding boxes
    bboxes_for_iou = para_bboxes_array[:, [0, 2]].reshape(-1, 4).tolist()
    areas_for_iou = [Polygon(bbox).area for bbox in para_bboxes_array]
    bbox_to_keep = approx_intersection_suppression(bboxes_for_iou, areas_for_iou, 0.95)

    if not for_spark:
        return bbox_to_keep, len(bbox_to_keep), len(para_bboxes_array)
    else:
        return Row("bbox_to_keep", "bbox_to_keep_count", "total_bbox_count")(bbox_to_keep, len(bbox_to_keep), len(para_bboxes_array))

def check_script_coverage(bboxes:Iterable, to_fail_threshold:float=0.5, failed_paras_ratio:float=0.3)->bool:
    """check_script_coverage Function to check coverage of language across paragraphs

    Args:
        bboxes (Iterable): List of bounding boxes with script and para information.
        to_fail_threshold (float, optional): Value below which a paragraph fails script coverage. Defaults to 0.5.
        failed_paras_ratio (float, optional): Value below which a document fails if the percentage of paragraph fail language script check. Defaults to 0.3.

    Returns:
        bool: Whether the given bounding boxes have the indicated language script.
    """
       
    paras_failed = 0
    total_paras = 0
    # Looping through the block list to get all the word-level lid results
    for block in bboxes:
        total_paras += len(block["paragraphs"])
        for paragraph in block["paragraphs"]:
            words_lid = []
            for word in paragraph["words"]:
                if not word["property"]:
                    continue
                langs_detected = word["property"]["detectedLanguages"]
                word_langs = []
                word_lang_confs = []
                for lang_conf in langs_detected:
                    word_langs += [lang_conf["languageCode"]]
                    word_lang_confs += [lang_conf["confidence"]]
                conf_idx = np.argmax(word_lang_confs)
                majority_word_lang = word_langs[conf_idx]
                majority_word_lang_conf = word_lang_confs[conf_idx]

                words_lid += [[majority_word_lang, majority_word_lang_conf]]

            total_words = len(words_lid)
            
            if not total_words:
                continue    
                
            words_lid_df = pd.DataFrame(words_lid, columns=["lang", "confidence"])
            words_count_df = words_lid_df.groupby(["lang"]).count()
            majority_word_count = words_count_df.loc[words_count_df.idxmax()]["confidence"].tolist()[0]
                    
            if majority_word_count / total_words < to_fail_threshold:
                paras_failed += 1
    
    if paras_failed/total_paras >= failed_paras_ratio:
        lid_check = False
    else:
        lid_check = True

    return lid_check

def check_plane_coverage(bboxes, bbox_to_keep, type="vertical"):

    def merge_bins(bins):
        order = sorted(range(len(bins)), key=lambda i: bins[i][1] - bins[i][0], reverse=True)
        keep = []
        while order:
            i = order.pop(0)
            keep.append(i)
            for j in order:
                if ( bins[i][0] <= bins[j][0] <= bins[i][1] ) and ( bins[i][0] <= bins[j][1] <= bins[i][1] ):
                    order.remove(j)
                elif ( bins[i][0] <= bins[j][0] <= bins[i][1] ):
                    bins[i][1] = bins[j][1]
                    order.remove(j)
                elif ( bins[i][0] <= bins[j][1] <= bins[i][1] ):
                    bins[i][0] = bins[j][0]
                    order.remove(j)

        bins = [bins[i] for i in keep]
        return bins
    
    # Looping through the block list to get all the paragraph-level bounding boxes
    paragraph_bboxes = []
    for block in bboxes:
        for paragraph in block["paragraphs"]:
            para_bbox = []
            for coords in paragraph["boundingBox"]["normalizedVertices"]:
                x, y = 0.0, 0.0
                if coords["x"]:
                    x = coords["x"]
                if coords["y"]:
                    y = coords["y"]
                para_bbox += [(x, y)]
            paragraph_bboxes += [para_bbox]

    if type == "horizontal":
        coordinate_idx = 0
    elif type == "vertical":
        coordinate_idx = 1
    
    para_bboxes_array = np.array(paragraph_bboxes)[bbox_to_keep][:, [0, 2], coordinate_idx].tolist()

    bins = [[para_bboxes_array[0][0], para_bboxes_array[0][1]]]

    for bbox in para_bboxes_array[1:]:
        create_new_bin = None
        for i, bin in enumerate(bins):
            create_new_bin = False
            if ( bin[0] <= bbox[0] <= bin[1] ) and ( bin[0] <= bbox[1] <= bin[1] ):
                break
            elif ( bin[0] <= bbox[0] <= bin[1] ):
                bins[i][1] = bbox[1]
                break
            elif ( bin[0] <= bbox[1] <= bin[1] ):
                bins[i][0] = bbox[0]
                break
            else:
                create_new_bin = True
        if create_new_bin:
            bins += [[bbox[0], bbox[1]]]

        bins = merge_bins(bins)

    total_plane_coverage = 0
    for bin in bins:
        total_plane_coverage += bin[1] - bin[0] 

    return total_plane_coverage

def check_bbox_overlap(bboxes, bbox_to_keep):

    def overlap_detection(boxes, scores):
        # Sort the boxes by score in descending order
        order = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)
        # keep = []
        ious = []
        while order:
            i = order.pop(0)
            # keep.append(i)
            for j in order:
                # Calculate the IoU between the two boxes
                intersection = py_builtin.max(0, py_builtin.min(boxes[i][2], boxes[j][2]) - py_builtin.max(boxes[i][0], boxes[j][0])) * \
                               py_builtin.max(0, py_builtin.min(boxes[i][3], boxes[j][3]) - py_builtin.max(boxes[i][1], boxes[j][1]))
                union = scores[i] + scores[j] - intersection

                if not union:
                    continue
                iou = intersection / union
                ious += [iou]

        if len(ious):
            max_iou = py_builtin.max(ious)
            return max_iou
        else:
            return None

    # Looping through the block list to get all the paragraph-level bounding boxes
    paragraph_bboxes = []
    for block in bboxes:
        for paragraph in block["paragraphs"]:
            para_bbox = []
            for coords in paragraph["boundingBox"]["normalizedVertices"]:
                x, y = 0.0, 0.0
                if coords["x"]:
                    x = coords["x"]
                if coords["y"]:
                    y = coords["y"]
                para_bbox += [(x, y)]
            paragraph_bboxes += [para_bbox]
    
    para_bboxes_array = np.array(paragraph_bboxes)[bbox_to_keep]
    
    # Using `approx_intersection_suppression` to remove EXTREMELY overlapping bounding boxes
    bboxes_for_iou = para_bboxes_array[:, [0, 2]].reshape(-1, 4).tolist()
    areas_for_iou = [Polygon(bbox).area for bbox in para_bboxes_array]
    max_iou = overlap_detection(bboxes_for_iou, areas_for_iou)
    return max_iou

def check_block_density(bboxes, bbox_to_keep):
    block_density = []
    paragraph_idx = 0
    for i, block in enumerate(bboxes):
        block_coords = []
        for coords in block["boundingBox"]["normalizedVertices"]:
            x, y = 0.0, 0.0
            if coords["x"]:
                x = coords["x"]
            if coords["y"]:
                y = coords["y"]
            block_coords += [(x, y)]  
        paragraph_bboxes = []   
        for j, paragraph in enumerate(block["paragraphs"]):
            if paragraph_idx not in bbox_to_keep:
                paragraph_idx += 1
                continue
            para_bbox = []
            for coords in paragraph["boundingBox"]["normalizedVertices"]:
                x, y = 0.0, 0.0
                if coords["x"]:
                    x = coords["x"]
                if coords["y"]:
                    y = coords["y"]
                para_bbox += [(x, y)]
            paragraph_bboxes += [para_bbox]
            paragraph_idx += 1
        
        block_polygon_area = Polygon(block_coords).area
        polygon_areas = [Polygon(bbox).area for bbox in paragraph_bboxes]
        total_paragraph_area = sum(polygon_areas)
        if block_polygon_area and total_paragraph_area:
            block_density += [total_paragraph_area/block_polygon_area]

    min_block_density = None
    if len(block_density):
        min_block_density = min(block_density)
    return min_block_density

def parse_ocr_output(bboxes, bbox_to_keep): 
    responseText = ""
    paragraph_bboxes = []
    for block in bboxes:
        for paragraph in block["paragraphs"]:
            paragraph_bboxes += [paragraph]

    paragraph_bboxes = [paragraph_bboxes[i] for i in sorted(bbox_to_keep)]

    for paragraph in paragraph_bboxes:
        paragraphText = ""
        for word in paragraph["words"]:
            wordText = ""
            for symbol in word["symbols"]:
                wordText += symbol["text"]
                breakCharacter = ""
                if symbol["property"]:
                    if symbol["property"]["detectedBreak"]:
                        if symbol["property"]["detectedBreak"]["type"]:
                            breakCharacter = symbol["property"]["detectedBreak"]["type"]
                if breakCharacter in {"SPACE", "EOL_SURE_SPACE"}:
                    wordText += " "
                elif breakCharacter == "LINE_BREAK":
                    wordText += "\n"
            
            paragraphText += wordText
        responseText += paragraphText
        
    return responseText

def get_ocr_url(uri, page_nos):
    gs_removed = uri.replace("gs://sangraha_pdfs/", "")
    base, filename = os.path.split(gs_removed)
    lang, identifier = os.path.split(base)
    first_page_no = page_nos.split(",")[0]
    url = f"https://archive.org/download/{identifier}/{filename}#page={first_page_no}"
    return url