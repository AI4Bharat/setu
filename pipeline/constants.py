import os
import glob
import multiprocessing
from nltk.corpus import stopwords

PIPELINE_ROOT_DIR = os.path.dirname(    # pipeline
                                    __file__)

MIN_WORDS = 3
MAX_WORDS = 8
MIN_STOP_WORDS = 2
NUM_PROCESSES = multiprocessing.cpu_count()

PARTIAL_JUNK_PATTERNS = [
    "lorem ipsum dolor"
]
COMPLETE_JUNK_PATTERNS = [
    "Loading",
    "Like",
    "Share",
    "Subscribe",
    "Continue Reading",
    "#"
]
TERMINAL_PUNCTUATIONS = (".", "!", "?", "।")  # TODO: See if string / nltk can be more extensive
# LANGS = [os.path.splitext(os.path.basename(g))[0] for g in glob.glob("filter_data/nsfw/*")]
LANGS = [os.path.splitext(os.path.basename(g))[0] for g in glob.glob(os.path.join(os.path.dirname(PIPELINE_ROOT_DIR), "dashboard/csv/*"))]
print(LANGS)

print("Initialising lookups...")
# STOPWORDS = {"english": list(set(stopwords.words("english")))}
STOPWORDS = {}
NSFW_WORDS = {}
PARTIAL_JUNK_PATTERNS = {}
COMPLETE_JUNK_PATTERNS = {}

for lang in glob.glob(os.path.join(PIPELINE_ROOT_DIR, "filter_data/nsfw/*.txt")):
    file_name = os.path.basename(lang)
    with open(os.path.join(PIPELINE_ROOT_DIR, f"filter_data/nsfw/{file_name}"), "r") as f:
        NSFW_WORDS[os.path.splitext(file_name)[0]] = list(map(lambda x: x.strip(), f.readlines()))

for lang in glob.glob(os.path.join(PIPELINE_ROOT_DIR, "filter_data/junk/partial/*.txt")):
    file_name = os.path.basename(lang)
    with open(os.path.join(PIPELINE_ROOT_DIR, f"filter_data/junk/partial/{file_name}"), "r") as f:
        PARTIAL_JUNK_PATTERNS[os.path.splitext(file_name)[0]] = list(map(lambda x: x.strip(), f.readlines()))

for lang in glob.glob(os.path.join(PIPELINE_ROOT_DIR, "filter_data/junk/complete/*.txt")):
    file_name = os.path.basename(lang)
    with open(os.path.join(PIPELINE_ROOT_DIR, f"filter_data/junk/complete/{file_name}"), "r") as f:
        COMPLETE_JUNK_PATTERNS[os.path.splitext(file_name)[0]] = list(map(lambda x: x.strip(), f.readlines()))

for lang in glob.glob(os.path.join(PIPELINE_ROOT_DIR, "filter_data/stopwords/*.txt")):
    file_name = os.path.basename(lang)
    with open(os.path.join(PIPELINE_ROOT_DIR, f"filter_data/stopwords/{file_name}"), "r") as f:
        STOPWORDS[os.path.splitext(file_name)[0]] = list(map(lambda x: x.strip(), f.readlines()))

print("Done...")

FILTERS = {
    lang: {
        "junk_partial": PARTIAL_JUNK_PATTERNS.get(lang, []),
        "junk_complete": COMPLETE_JUNK_PATTERNS.get(lang, []),
        "stopwords": STOPWORDS.get(lang, []),
        "nsfw_words": NSFW_WORDS.get(lang, []),
    } for lang in LANGS
}

print(FILTERS)

