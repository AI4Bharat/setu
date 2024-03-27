import os
import glob
import multiprocessing
from nltk.corpus import stopwords
from flashtext import KeywordProcessor
import traceback

class Constants():

    """
    The Constants Class contains different types of variables and lists such languages, nsfw words, stop words, punctuations, non indic latin patterns, junk patterns, etc.

    :param filter_data_root: Path to the data file containing various filters
    :type filter_data_root: str
    """

    def __init__(
        self,
        filter_data_root:str, 
        junk_partial_glob:str = "junk/partial/*.txt", 
        junk_complete_glob:str = "junk/complete/*.txt", 
        stopwords_glob:str = "stopwords/*.txt", 
        nsfw_words_glob:str = "nsfw/*.txt",
        langs:list = [
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
    )->None:
        """__init__ Initializes the Constants class 

        Args:
            filter_data_root (str): Data path for the available filters
            junk_partial_glob (str, optional): Data path for the junk partial texts. Defaults to "junk/partial/*.txt".
            junk_complete_glob (str, optional): Data path for the junk complete texts. Defaults to "junk/complete/*.txt".
            stopwords_glob (str, optional): Data path for the stopword text files. Defaults to "stopwords/*.txt".
            nsfw_words_glob (str, optional): Data path for the NSFW text files. Defaults to "nsfw/*.txt".
            langs (list, optional): Language list. Defaults to [ "assamese", "bengali", "bodo", "dogri", "english", "gujarati", "hindi", "kannada", "kashmiri", "konkani", "maithili", "malayalam", "marathi", "manipuri", "nepali", "oriya", "punjabi", "sanskrit", "santhali", "sindhi", "tamil", "telugu", "urdu", "other", ].
        """
        # if not os.path.isdir(filter_data_root):
        #     raise ValueError(f"`filter_data_root`={filter_data_root} which is either None or not a directory. Please properly set FILTER_DATA_ROOT environment variable.")

        self.filter_data_root = filter_data_root
        """The filter data root path."""

        self.junk_partial_glob = junk_partial_glob
        """The junk partial texts path."""
        self._partial_junk_patterns = {}
        """The junk partial patterns."""
        self.load_junk_partial(junk_partial_glob)

        self.junk_complete_glob = junk_complete_glob
        """The junk complete texts path."""
        self._complete_junk_patterns = {}
        """The junk complete patterns."""
        self.load_junk_complete(junk_complete_glob)

        self.stopwords_glob = stopwords_glob
        """The stopwords texts path."""
        self._stopwords = {}
        """The list of stopwords for each language"""
        self.load_stop_words(stopwords_glob)

        self.nsfw_words_glob = nsfw_words_glob
        """The nsfw texts path."""
        self._nsfw_words = {}
        """The list of nsfw words for each language"""
        self.load_nsfw_words(nsfw_words_glob)

        self.FILTER_WORDS = {
            lang: {
                "junk_partial": self._partial_junk_patterns.get(lang, []),
                "junk_complete": self._complete_junk_patterns.get(lang, []),
                "stopwords": self._stopwords.get(lang, []),
                "nsfw_words": self._nsfw_words.get(lang, []),
            } for lang in langs
        }
        """Filter Words mapping for each language"""

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

        """Non Latin Indic Patterns"""
        # This gets converted into a single raw string. Python's functionality

        self.TERMINAL_PUNCTUATIONS = (
            ".", "!", "?", "।", "।।", ":", ",", ";", ")", "\"", "\'",
            "؟", "۔" # this 2 are specifically for Urdu.
        ) # TODO: See if string / nltk can be more extensive
        """List of terminal punctuations"""
        # chunks ending with these patterns should be completely removed.
        self.TERMINAL_PUNCTUATIONS_EXCEPTION = (
            "...",
            "####",
        )
        """List of terminal punctuation exceptions"""
        self.MIN_WORDS = 3
        """Minimum number of words to be present"""
        self.MAX_WORDS = 8
        """Maximum number of words to be present"""
        self.MIN_STOP_WORDS = 2
        """Minimum number of stopwords to be present"""
        self.CPU_COUNT = multiprocessing.cpu_count()
        """CPU Count for multiprocessing"""


    def load_junk_partial(self, junk_partial_glob:str):
        """load_junk_partial Loads the Junk Partial Texts

        Args:
            junk_partial_glob (str): Path to Junk Partial text files.
        """
        for lang_file in glob.glob(os.path.join(self.filter_data_root, junk_partial_glob)):
            file_name = os.path.basename(lang_file)
            with open(lang_file, "r") as f:
                self._partial_junk_patterns[os.path.splitext(file_name)[0]] = list(map(lambda x: x.strip(), f.readlines()))

    def load_junk_complete(self, junk_complete_glob:str):
        """load_junk_complete Loads the Junk Complete Texts

        Args:
            junk_complete_glob (str): Path to Junk Complete text files.
        """
        for lang_file in glob.glob(os.path.join(self.filter_data_root, junk_complete_glob)):
            file_name = os.path.basename(lang_file)
            with open(lang_file, "r") as f:
                self._complete_junk_patterns[os.path.splitext(file_name)[0]] = list(map(lambda x: x.strip(), f.readlines()))

    def load_stop_words(self, stopwords_glob:str):
        """load_stop_words Loads the list of stopwords

        Args:
            stopwords_glob (str): Path to Stopword text files.
        """
        for lang_file in glob.glob(os.path.join(self.filter_data_root, stopwords_glob)):
            file_name = os.path.basename(lang_file)
            with open(lang_file, "r") as f:
                self._stopwords[os.path.splitext(file_name)[0]] = list(map(lambda x: x.strip(), f.readlines()))

    def load_nsfw_words(self, nsfw_words_glob:str):
        """load_nsfw_words Loads the list of nsfw words

        Args:
            msfw_words_glob (str): Path to nsfw word text files.
        """
        for lang_file in glob.glob(os.path.join(self.filter_data_root, nsfw_words_glob)):
            file_name = os.path.basename(lang_file)
            with open(lang_file, "r") as f:
                self._nsfw_words[os.path.splitext(file_name)[0]] = list(map(lambda x: x.strip(), f.readlines()))

def create_kwpr(keywords:dict)->KeywordProcessor:
    """create_kwpr Creates a flashtext Keywordprocessor given a list of keywords

    Args:
        keywords (dict): Dictionary containing all the keywords

    Raises:
        Exception: Throws exception if not a dict or list object

    Returns:
        KeywordProcessor: KeywordProcessor loaded with required keywords
    """
    keyword_processor = KeywordProcessor()
    if isinstance(keywords, dict):
        keyword_processor.add_keywords_from_dict(keywords)
    elif isinstance(keywords, list):
        keyword_processor.add_keywords_from_list(keywords)
    else:
        raise Exception("Please send keywords as a dict or list")
    return keyword_processor


def init_kwprs(constants: Constants)->dict:
    """init_kwprs Initialize the keword processors given the various constants.

    Args:
        constants (Constants): Constants class object

    Returns:
        dict: Dictionary containing the keyword processors
    """
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
    """Constants Class Object"""
    KW_PROCESSORS = init_kwprs(CONSTANTS)
except Exception as e:
    print(f"Please set `FILTER_DATA_ROOT` environment variable properly. Getting error: {e}")
    traceback.print_exc()
    raise Exception("Cannot initiate Setu pipeline due to above error")