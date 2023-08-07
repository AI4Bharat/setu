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

st.set_page_config(layout="wide")

PIPELINE_ROOT_DIR = os.path.dirname(    # dashboard
                        os.path.dirname(    # versions
                            os.path.dirname(    # v4
                                os.path.dirname(__file__))))

sys.path.insert(0, PIPELINE_ROOT_DIR)

from setu.setu import Setu

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
        
    return html_str

@st.cache_data
def extract_using_trafilatura(html_str):
    cleaned_text = trafilatura.bare_extraction(html_str, include_images=False)
    return cleaned_text

@st.cache_resource
def load_setu(
    config_file="configs/dashboard_config.json",
):
    return Setu(config_file=config_file)

SETU = load_setu(config_file=os.path.join(PIPELINE_ROOT_DIR, "setu", "configs", "dashboard_config.json"))

ROOT_DIR = os.path.join(PIPELINE_ROOT_DIR, "dashboard", "parquet")
WEBSITES = load_domain_list()
EOS_CLIENT = load_minio_client()

@st.cache_data
def run_setu(
    doc, 
    setu_config, 
    use_code_filter=True, 
    use_terminal_punc=True,
    enable_flagging=True,
):
    return SETU.run_pipeline(
        doc,
        use_code_filter=use_code_filter,
        use_terminal_punc_filter=use_terminal_punc,
        enable_analysis=True,
        enable_flagging=enable_flagging,
        lid_probability_threshold=setu_config["lid_probability_threshold"],
        chunk_len_threshold=2, # TODO: Not used in anymore. So, remove this.
        non_li_threshold=setu_config["non_li_char_threshold"],
        nsfw_threshold=setu_config["nsfw_threshold"],
        symbol_number_threshold=setu_config["symbol_numbers_threshold"],
        min_line_count=setu_config["min_line_count"],
        min_mean_line_len=setu_config["min_mean_line_len"],
        word_ngram_cum_thresholds=setu_config["word_ngram_cum_thresholds"],
        char_ngram_cum_thresholds=setu_config["char_ngram_cum_thresholds"]
    )

language_website_mapping, languages = load_language_website_mapping()

st.header("Setu Dashboard")

selected_language = st.selectbox("Select Language", languages)

if selected_language:
    domain_list = load_domain_list(selected_language)
else:
    domain_list = WEBSITES
selected_domain = st.selectbox("Select Domain", domain_list)

if selected_domain:
    urls_df, urls = load_url_list(ROOT_DIR, language=selected_language, domain=selected_domain)
selected_url = st.selectbox("Select URL", urls)

raw_html = download_objects(selected_url, _eos_client=EOS_CLIENT)

output_format_radio, raw_html_radio = st.columns(2)

with output_format_radio:
    trafilatura_output_format_as_boilerpipe = st.radio("Do you want same JSON structure as Boilerpipe for better comparison?", (True, False))

with raw_html_radio:
    raw_html_or_not = st.radio("Do you want to view raw html?", (False, True))

cleaned_text = {}
_trafilatura_data = {}

setu_config = {
    "lid_probability_threshold": 0.7,
    "chunk_len_threshold": 2,
    "nsfw_threshold": 1.0,
    "symbol_numbers_threshold": 1.0,
    "non_li_char_threshold": 1.0,
    "min_line_count": 0,
    "min_mean_line_len": 0,
    "word_ngram_cum_thresholds": {},
    "char_ngram_cum_thresholds": {},
}

use_code_filter, use_terminal_punc, enable_flagging, view_option = True, True, True, "trafilatura"

with st.sidebar:

    st.header("Setu Config")

    code_filter_or_not_col, terminal_punc_or_not_col, flagging_or_not_col = st.columns(3)
    word_ngram_cum_thresholds_col, char_ngram_cum_thresholds_col = st.columns(2)

    with code_filter_or_not_col:
        use_code_filter = bool(st.radio("Perform Code Filtering?", (True, False)))

    with terminal_punc_or_not_col:
        use_terminal_punc = bool(st.radio("Perform Terminal Punctuation Filtering?", (True, False)))

    with flagging_or_not_col:
        enable_flagging = bool(st.radio("Flag Document?", (True, False)))  

    with word_ngram_cum_thresholds_col:
        word_ngram_range = st.slider("word_ngrams", 0, 20, (5, 5), key="word_ngrams")
        if enable_flagging:
            threshold_dict = {}
            for i in range(word_ngram_range[0], word_ngram_range[1]+1):
                if f'{i}_word_ngram_threshold' not in list(st.session_state.keys()):
                    st.session_state[f'{i}_word_ngram_threshold'] = 0.3
                threshold = float(st.slider(f"{i}-ngram word repetition threshold",
                                            0.0, 1.0,
                                            st.session_state[f'{i}_word_ngram_threshold'], 
                                            key=f"word {i}-ngram repetition threshold"))
                if threshold != st.session_state[f'{i}_word_ngram_threshold']:
                    st.session_state[f'{i}_word_ngram_threshold'] = threshold

                threshold_dict[f"{i}"] = threshold
                
            setu_config["word_ngram_cum_thresholds"] = threshold_dict
                
    with char_ngram_cum_thresholds_col:
        char_ngram_range = st.slider("char_ngrams", 0, 50, (10,10))
        if enable_flagging:
            threshold_dict = {}
            for i in range(char_ngram_range[0], char_ngram_range[1]+1):
                if f'{i}_char_ngram_threshold' not in list(st.session_state.keys()):
                    st.session_state[f'{i}_char_ngram_threshold'] = 0.15
                threshold = float(st.slider(f"{i}-ngram char repetition threshold", 
                                            0.0, 1.0,
                                            st.session_state[f'{i}_char_ngram_threshold'],
                                            key=f"char {i}-ngram repetition threshold"))
                if threshold != st.session_state[f'{i}_char_ngram_threshold']:
                    st.session_state[f'{i}_char_ngram_threshold'] = threshold

                threshold_dict[f"{i}"] = threshold
                
            setu_config["char_ngram_cum_thresholds"] = threshold_dict
        
    if enable_flagging:

        st.text("lid_probability_threshold")
        setu_config["lid_probability_threshold"] = float(st.slider('lid_probability_threshold', 0.0, 1.0, 0.7, key="lid_probability_threshold", label_visibility="hidden"))

        st.text("nsfw_threshold")
        setu_config["nsfw_threshold"] = float(st.slider('nsfw_threshold', 0.0, 1.0, 1.0, key="nsfw_threshold", label_visibility="hidden"))

        st.text("symbol_numbers_threshold")
        setu_config["symbol_numbers_threshold"] = float(st.slider('symbol_numbers_threshold', 0.0, 1.0, 1.0, key="symbol_numbers_threshold", label_visibility="hidden"))

        st.text("non_li_char_threshold")
        setu_config["non_li_char_threshold"] = float(st.slider('non_li_char_threshold', 0.0, 1.0, 1.0, key="non_li_char_threshold", label_visibility="hidden"))

        st.text("min_line_count")
        setu_config["min_line_count"] = int(st.slider('min_line_count', 0, 10, 0, key="min_line_count", label_visibility="hidden"))

        st.text("min_mean_line_len")
        setu_config["min_mean_line_len"] = int(st.slider('min_mean_line_len', 0, 10, 0, key="min_mean_line_len", label_visibility="hidden"))

        st.subheader("Selected Setu Config")

        _ = setu_config.pop("chunk_len_threshold") # TODO: Remove chunk_len_threshold entirely

        st.write(setu_config)

if bool(raw_html_or_not):
    st.write(raw_html)
else:
    raw_column, extracted_col_A = st.columns(2)
    with raw_column:
        st.subheader("Webpage")
        components.iframe(raw_html["url"], height=1000, scrolling=True)

    with extracted_col_A:
        st.subheader("Extracted Text")
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

        cleaned_text["id"] = os.path.split(selected_url)[1]
        _trafilatura_data = cleaned_text
        _trafilatura_data["text"] = _trafilatura_data.pop("body")

    extracted_col_B, cleaned_col = st.columns(2)

    with extracted_col_B:
        st.subheader("Extracted Text")
        tmp_json = {
            "text": cleaned_text["text"],
        }
        st.json(tmp_json)

    with cleaned_col:
        st.subheader("Cleaned Text")
        trafilatura_output, _trafilatura_data["code_span_cleaned_text"], _trafilatura_data["terminal_cleaned_text"], _trafilatura_data["lines"] = run_setu(
            doc=_trafilatura_data,
            setu_config=setu_config,
            use_code_filter=use_code_filter,
            use_terminal_punc=use_terminal_punc,
            enable_flagging=enable_flagging
        )

        tmp_json = {
            "text": trafilatura_output["analysis"]["text"],
        }
        st.json(tmp_json, expanded=True)

    cleaned_text_col, lines_col = st.columns(2)

    with cleaned_text_col:
        st.subheader("Cleaned Text")
        tmp_json = {
            "text": trafilatura_output["analysis"]["text"],
        }
        st.json(tmp_json, expanded=True)

    with lines_col:
        st.subheader("Sentence Tokenization")
        st.json(_trafilatura_data["lines"])

    analysis_col, flag_col = st.columns(2)

    with analysis_col:

        st.subheader("Analysis of Cleaned Text")

        analysis_json = {
            "lid output": {
                "majority": trafilatura_output["analysis"]["lid_major"],
                "all": trafilatura_output["analysis"]["lid_all"],
            },
            "number of lines": trafilatura_output["analysis"]["num_of_lines"],
            "nsfw word count": trafilatura_output["analysis"]["stats"]["nsfw_words_count"],
            "symbol numbers count": trafilatura_output["analysis"]["stats"]["symbol_numbers_count"],
            "non latin/indic character count": trafilatura_output["analysis"]["stats"]["non_li_count"],
            "byte size of document": trafilatura_output["analysis"]["stats"]["bytes"],
            "total word count": trafilatura_output["analysis"]["stats"]["words_count"],
            "total character count": trafilatura_output["analysis"]["stats"]["char_count"],
            "line length statistics": trafilatura_output["analysis"]["line_length_stats"],
            "code spans found": trafilatura_output["analysis"]["code_spans"],
            "word n-gram repetition scores": {},
            "character n-gram repetition scores": {},
        }

        for key in trafilatura_output["analysis"]["repeated_ngram_dist"]:
            
            for inner_key, ngrams in [("word", "word_ngram_cum_thresholds"), ("character", "char_ngram_cum_thresholds")]:

                for ng in setu_config[ngrams].keys():
                    
                    analysis_json[f"{inner_key} n-gram repetition scores"][ng] = trafilatura_output["analysis"]["repeated_ngram_dist"][inner_key][f"{ng}_gram_{inner_key}s_repetition_score"]

        st.json(analysis_json)

    with flag_col:

        st.subheader("Flags for Cleaned Text")
        
        if enable_flagging:

            flags_json = trafilatura_output["flags"]

            st.json(flags_json)
        
        else:
            st.write("Flagging is disabled")

        

