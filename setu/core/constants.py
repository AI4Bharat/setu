import os
import glob
import multiprocessing
from nltk.corpus import stopwords
from flashtext import KeywordProcessor
import traceback

class Constants():

    def __init__(
        self,
        filter_data_root, 
        junk_partial_glob = "junk/partial/*.txt", 
        junk_complete_glob = "junk/complete/*.txt", 
        stopwords_glob = "stopwords/*.txt", 
        nsfw_words_glob = "nsfw/*.txt",
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
            "marathi",
            "manipuri",
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
    ):
        
        # if not os.path.isdir(filter_data_root):
        #     raise ValueError(f"`filter_data_root`={filter_data_root} which is either None or not a directory. Please properly set FILTER_DATA_ROOT environment variable.")

        self.filter_data_root = filter_data_root

        self.junk_partial_glob = junk_partial_glob
        self._partial_junk_patterns = {}
        self.load_junk_partial(junk_partial_glob)

        self.junk_complete_glob = junk_complete_glob
        self._complete_junk_patterns = {}
        self.load_junk_complete(junk_complete_glob)

        self.stopwords_glob = stopwords_glob
        self._stopwords = {}
        self.load_stop_words(stopwords_glob)

        self.nsfw_words_glob = nsfw_words_glob
        self._nsfw_words = {}
        self.load_nsfw_words(nsfw_words_glob)

        self.FILTER_WORDS = {
            lang: {
                "junk_partial": self._partial_junk_patterns.get(lang, []),
                "junk_complete": self._complete_junk_patterns.get(lang, []),
                "stopwords": self._stopwords.get(lang, []),
                "nsfw_words": self._nsfw_words.get(lang, []),
            } for lang in langs
        }

        self.non_indic_non_latin_re_pattern = (
            r'[^'
            r'\p{Script=Latin}'
            r'\p{Script=Devanagari}'
            r'\p{Script=Bengali}'
            r'\p{Script=Gujarati}'
            r'\p{Script=Gurmukhi}'
            r'\p{Script=Kannada}'
            r'\p{Script=Malayalam}'
            r'\p{Script=Oriya}'
            r'\p{Script=Tamil}'
            r'\p{Script=Telugu}'
            r'\p{Script=Meetei Mayek}'
            r'\p{Script=Arabic}'
            r'\p{Script=Dogra}'
            r'\p{Script=Ol Chiki}'
            r'\p{P}'
            r'\s'
            r']'
        )
        # This gets converted into a single raw string. Python's functionality

        self.TERMINAL_PUNCTUATIONS = (
            ".", "!", "?", "।", "।।", ":", ",", ";", ")", "\"", "\'",
            "؟", "۔" # this 2 are specifically for Urdu.
        ) # TODO: See if string / nltk can be more extensive

        # chunks ending with these patterns should be completely removed.
        self.TERMINAL_PUNCTUATIONS_EXCEPTION = (
            "...",
            "####",
        )

        self.MIN_WORDS = 3
        self.MAX_WORDS = 8
        self.MIN_STOP_WORDS = 2
        self.CPU_COUNT = multiprocessing.cpu_count()


    def load_junk_partial(self, junk_partial_glob):
        for lang_file in glob.glob(os.path.join(self.filter_data_root, junk_partial_glob)):
            file_name = os.path.basename(lang_file)
            with open(lang_file, "r") as f:
                self._partial_junk_patterns[os.path.splitext(file_name)[0]] = list(map(lambda x: x.strip(), f.readlines()))

    def load_junk_complete(self, junk_complete_glob):
        for lang_file in glob.glob(os.path.join(self.filter_data_root, junk_complete_glob)):
            file_name = os.path.basename(lang_file)
            with open(lang_file, "r") as f:
                self._complete_junk_patterns[os.path.splitext(file_name)[0]] = list(map(lambda x: x.strip(), f.readlines()))

    def load_stop_words(self, stopwords_glob):
        for lang_file in glob.glob(os.path.join(self.filter_data_root, stopwords_glob)):
            file_name = os.path.basename(lang_file)
            with open(lang_file, "r") as f:
                self._stopwords[os.path.splitext(file_name)[0]] = list(map(lambda x: x.strip(), f.readlines()))

    def load_nsfw_words(self, nsfw_words_glob):
        for lang_file in glob.glob(os.path.join(self.filter_data_root, nsfw_words_glob)):
            file_name = os.path.basename(lang_file)
            with open(lang_file, "r") as f:
                self._nsfw_words[os.path.splitext(file_name)[0]] = list(map(lambda x: x.strip(), f.readlines()))

def create_kwpr(keywords):
    keyword_processor = KeywordProcessor()
    if isinstance(keywords, dict):
        keyword_processor.add_keywords_from_dict(keywords)
    elif isinstance(keywords, list):
        keyword_processor.add_keywords_from_list(keywords)
    else:
        raise Exception("Please send keywords as a dict or list")
    return keyword_processor


def init_kwprs(constants: Constants):
    kw_processors = {}
    for lang, values in constants.FILTER_WORDS.items():
        kw_processors[lang] = {}
        for flter, kws in values.items():
            kw_processors[lang][flter] = create_kwpr(kws)
    return kw_processors

try:
    CONSTANTS = Constants(
        filter_data_root= "/opt/setu/filter_data/filter_data" if not os.getenv("FILTER_DATA_ROOT") else os.getenv("FILTER_DATA_ROOT"),
    )
    KW_PROCESSORS = init_kwprs(CONSTANTS)
except Exception as e:
    print(f"Please set `FILTER_DATA_ROOT` environment variable properly. Getting error: {e}")
    traceback.print_exc()
    raise Exception("Cannot initiate Setu pipeline due to above error")