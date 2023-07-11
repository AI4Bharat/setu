from contextlib import redirect_stdout, redirect_stderr
import io
import sys
import subprocess
import traceback
import pandas as pd
import streamlit as st
from trafilatura.settings import use_config
import trafilatura
import json
import os
import glob
import minio
from minio import Minio
import streamlit.components.v1 as components
from boilerpipe.extract import Extractor

PIPELINE_ROOT_DIR = os.path.dirname(    # dashboard
                        os.path.dirname(    # versions
                            os.path.dirname(    # v2
                                os.path.dirname(__file__))))

sys.path.insert(0, os.path.join(
    PIPELINE_ROOT_DIR, "pipeline"))

from language_identification.lid_pipeline import LIDPipeline

st.set_page_config(layout="wide")

@st.cache_data
def load_language_website_mapping():
    with open(os.path.join(os.path.dirname(ROOT_DIR), "lang_website_mapping.json")) as lang_website_f:
        language_website_mapping = json.load(lang_website_f)
    languages = tuple(language_website_mapping.keys())
    return language_website_mapping, languages

@st.cache_data
def load_domain_list(language="*", format=".parquet"):
    websites = tuple(map(lambda x: os.path.split(x[:-len(format)])[1], glob.glob(os.path.join(ROOT_DIR, language, f"*{format}"))))
    return websites

@st.cache_data
def load_url_list(root_dir, language="*", domain="*"):
    urls_df = pd.read_parquet(os.path.join(root_dir, language, f"{domain}.parquet"))
    urls = urls_df["identifier"].tolist()
    return urls_df, urls

@st.cache_resource
def load_minio_client():
    eos_client = Minio('objectstore.e2enetworks.net',
                        access_key='ZEBYMSQX84YO5SW8VIYV',
                        secret_key='YU3NO3JK2WLE37X97BRONFG3QJ643MTL70P9DPJ3',
                        secure=True)
    return eos_client

@st.cache_data
def download_objects(url, _eos_client):
    try:
        raw_response = _eos_client.get_object(
            bucket_name='ai4b-public-nlu-nlg',
            object_name=url,
        )
        html_str = json.loads(raw_response.data.decode())

    finally:
        raw_response.close()
        raw_response.release_conn()
        
    try:
        boilerpipe_response = _eos_client.get_object(
            bucket_name='ai4b-public-nlu-nlg',
            object_name=url.replace("/html/", "/articles/"),
        )
        boilerpipe_str = json.loads(boilerpipe_response.data.decode())
        boilerpipe_response.close()
        boilerpipe_response.release_conn()
    except minio.error.S3Error:
        boilerpipe_str = "Boilerpipe extracted text not found on S3. Boilerpipe didn't extract article for this webpage or it had not been uploaded yet!"
        
    return html_str, boilerpipe_str

@st.cache_data
def extract_using_trafilatura(html_str):
    cleaned_text = trafilatura.bare_extraction(html_str, include_images=False)
    return cleaned_text

@st.cache_resource
def load_lid(
    IndicLID_FTN_path='models/indiclid-ftn/model_baseline_roman.bin',
    IndicLID_FTR_path='models/indiclid-ftr/model_baseline_roman.bin',
    IndicLID_BERT_path='models/indiclid-bert/basline_nn_simple.pt',
    input_threshold=0.5,
    roman_lid_threshold=0.6, 
    nllb_model_path="models/lid218e.bin", 
    mapping_json_path="./language_mapping.json"):

    return LIDPipeline(
        IndicLID_FTN_path, IndicLID_FTR_path,
        IndicLID_BERT_path, input_threshold,
        roman_lid_threshold, nllb_model_path, 
        mapping_json_path
    )
    
@st.cache_data
def perform_lid(text):
    res = LID_PIPELINE.run_single(text)
    return res


ROOT_DIR = os.path.join(PIPELINE_ROOT_DIR, "dashboard", "parquet")
WEBSITES = load_domain_list()
EOS_CLIENT = load_minio_client()
LID_PIPELINE = load_lid(
    IndicLID_FTN_path=os.path.join(PIPELINE_ROOT_DIR, 'pipeline', 'language_identification', 'models', 'indiclid-ftn', 'model_baseline_roman.bin'),
    IndicLID_FTR_path=os.path.join(PIPELINE_ROOT_DIR, 'pipeline', 'language_identification', 'models', 'indiclid-ftr', 'model_baseline_roman.bin'),
    IndicLID_BERT_path=os.path.join(PIPELINE_ROOT_DIR, 'pipeline', 'language_identification', 'models', 'indiclid-bert', 'basline_nn_simple.pt'),
    input_threshold=0.5,
    roman_lid_threshold=0.6, 
    nllb_model_path=os.path.join(PIPELINE_ROOT_DIR, 'pipeline', 'language_identification', 'models', 'lid218e.bin'), 
    mapping_json_path=os.path.join(PIPELINE_ROOT_DIR, 'pipeline', 'language_identification', 'language_mapping.json'),
)

language_website_mapping, languages = load_language_website_mapping()

selected_language = st.selectbox("Select Language", languages)

if selected_language:
    domain_list = load_domain_list(selected_language)
else:
    domain_list = WEBSITES
selected_domain = st.selectbox("Select Domain", domain_list)

if selected_domain:
    urls_df, urls = load_url_list(ROOT_DIR, language=selected_language, domain=selected_domain)
selected_url = st.selectbox("Select URL", urls)

raw_html, boilerpipe_cleaned_text = download_objects(selected_url, _eos_client=EOS_CLIENT)

output_format_radio, raw_html_radio, reextract_pure_boilerpipe_radio = st.columns(3)

with output_format_radio:
    trafilatura_output_format_as_boilerpipe = st.radio("Do you want same JSON structure as Boilerpipe for better comparison?", (True, False))

with raw_html_radio:
    raw_html_or_not = st.radio("Do you want to view raw html?", (False, True))

with reextract_pure_boilerpipe_radio:
    pure_boilerpipe_or_not = st.radio("Do you want to re-extract using Boilerpipe without `ok-check`?", (True, False))

cleaned_text = {}
if bool(raw_html_or_not):
    st.write(raw_html)
else:
    raw_column, trafilatura_column, boilerpipe_column = st.columns(3)
    with raw_column:
        st.subheader("Raw")
        components.iframe(raw_html["url"], height=1000, scrolling=True)

    with boilerpipe_column:
        st.subheader("Boilerpipe")
        if bool(pure_boilerpipe_or_not):
            extractor = Extractor(extractor='ArticleExtractor',
                                  html=raw_html['html'])
            body = str(extractor.getText())
            title = str(extractor.source.getTitle())
            art = {
                'title': title,
                'body': body,
                'source': raw_html['source'],
                'url': raw_html['url'],
                'timestamp': raw_html['timestamp']
            }
            boilerpipe_cleaned_text = art

        st.write(boilerpipe_cleaned_text)

    with trafilatura_column:
        st.subheader("Trafilatura")
        if selected_url:
            try:
                trafilatura_cleaned_text = extract_using_trafilatura(raw_html["html"])
                if bool(trafilatura_output_format_as_boilerpipe):
                    cleaned_text["title"] = trafilatura_cleaned_text["title"]
                    cleaned_text["body"] = trafilatura_cleaned_text["text"]
                    cleaned_text["source"] = trafilatura_cleaned_text["sitename"]
                    cleaned_text["url"] = trafilatura_cleaned_text["url"]
                else:
                    cleaned_text = trafilatura_cleaned_text
                
                cleaned_text['timestamp'] = raw_html['timestamp']
                st.write(cleaned_text)
            except Exception as e:
                st.write("Error occurred during scraping:", str(e))
        else:
            st.write("Please select a URL to compare")

lid_section, _, _ = st.columns(3)

with lid_section:
    st.subheader("Language Identification")
    perform_lid_or_not = st.radio("Perform LID?", (True, False))
    use_trafilatura_or_boiler = st.radio("Choose which text extraction output to use?", ("Trafilatura", "Boilerpipe"))

    if bool(perform_lid_or_not):
        in_text = None
        if use_trafilatura_or_boiler == "Trafilatura":
            in_text = cleaned_text["body"]
        else:
            in_text = boilerpipe_cleaned_text["body"]
        res = perform_lid(in_text.replace("\n", ""))
        st.write(in_text)
        text_section, output_section = st.columns(2)
        with text_section:
            st.write("Identified Language/s: ")
        with output_section:
            st.write(res)