from flashtext import KeywordProcessor
from constants import Constants
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, IntegerType, ArrayType, MapType, StringType
from indicnlp.tokenize.sentence_tokenize import sentence_split
# from lid import LIDPipeline
import json

class LineFiltersPipeline():

    def __init__(
            self,
            # lid_pipeline=None
            filter_data_root
        ):
        self.constants = Constants(
            filter_data_root=filter_data_root
        )
        self.KW_PROCESSORS = self.init_kwprs()
        # if not lid_pipeline:
        #     self.lid_pipeline = LIDPipeline()
        # else:
        #     self.lid_pipeline = lid_pipeline

    def create_kwpr(self, keywords):
        keyword_processor = KeywordProcessor()
        if isinstance(keywords, dict):
            keyword_processor.add_keywords_from_dict(keywords)
        elif isinstance(keywords, list):
            keyword_processor.add_keywords_from_list(keywords)
        else:
            raise Exception("Please send keywords as a dict or list")
        return keyword_processor


    def init_kwprs(self):
        kw_processors = {}
        for lang, values in self.constants.FILTER_WORDS.items():
            kw_processors[lang] = {}
            for flter, kws in values.items():
                kw_processors[lang][flter] = self.create_kwpr(kws)
        return kw_processors

    # Step 1: Filter lines without terminal punctuation
    def is_terminal_punctuation(self, line):
        if line.endswith(self.constants.TERMINAL_PUNCTUATIONS):
            return True
        return False
    
    # Step 2: Filter lines based on word count range
    def get_word_count(self, line):
        return len(line.split())


    # Step 3: Filter lines based on junk content
    def is_junk_content(self, line, lang):
        extracted_kws = self.KW_PROCESSORS[lang]["junk_partial"].extract_keywords(line)
        if extracted_kws:
            return True

        extracted_kws = self.KW_PROCESSORS[lang]["junk_complete"].extract_keywords(line, span_info=True)
        for word, start, end in extracted_kws:
            if start == 0 and end == len(line):
                return True
        return False


    # Step 4: Filter lines containing only numbers
    def is_numbers(self, line):
        if line.strip().isdigit():
            return True
        return False


    # Step 5: Filter lines based on stop words count
    def get_stop_word_dist(self, line, lang):
        extracted_kws = self.KW_PROCESSORS[lang]["stopwords"].extract_keywords(line, span_info=True)
        word_dist = {}
        for word, start, end in extracted_kws:
            if word in word_dist:
                word_dist[word] += 1
            else:
                word_dist[word] = 1
        return word_dist


    def get_nsfw_word_dist(self, line, lang):
        extracted_kws = self.KW_PROCESSORS[lang]["nsfw_words"].extract_keywords(line, span_info=True)
        word_positions = [(start, end) for _, start, end in extracted_kws]
        return word_positions
    
    def split_to_lines(self, text, lang):
        return sentence_split(text, lang)

    def get_stopword_count(self, stop_word_dist):
        stopword_count = sum(stop_word_dist.values())
        return stopword_count

    def extract_line_level_metadata(self, text, lang, iso_code):

        input_lines = self.split_to_lines(text, iso_code)
        
        newline_cleaned_input_lines = list(map(lambda x: x.replace("\n", " "), input_lines))
        # lid_res = self.lid_pipeline.run_batch(newline_cleaned_input_lines)
        results = []
        for j, line in enumerate(input_lines):
            results.append(self._extract_line_level_metadata(j, line, lang, 
                                                            #  lid_res[j]
                                                            ))
        return results, input_lines
    

    def _extract_line_level_metadata(self, line_no, line, lang,
                                    # lid_res,
                                    doc_id="id"):
        line = line.strip()

        fmt = {
            "identifier": doc_id,                                                       # str
            "line_no": line_no,                                                         # int
            "term_valid": self.is_terminal_punctuation(line),                           # bool
            "is_junk": self.is_junk_content(line, " english"),                           # bool
            "is_num": self.is_numbers(line),                                            # bool
            "word_count": self.get_word_count(line),                                    # int
            "stop_word_distribution": self.get_stop_word_dist(line, lang),              # Dict[str, int],
            "nsfw_span": self.get_nsfw_word_dist(line, "english"),                      # List[Tuple(int, int)],
        }
        fmt["stopword_count"] = self.get_stopword_count(fmt["stop_word_distribution"])  # int
        # fmt["stop_word_distribution"] = json.dumps(fmt["stop_word_distribution"])       
        # fmt["nsfw_span"] = json.dumps(fmt["nsfw_span"])
        return fmt

class LineFiltersSparkPipeline(LineFiltersPipeline):

    def __init__(self):
        super().__init__(

        )

        # Initialize SparkSession
        self.spark = SparkSession.builder.getOrCreate()

        self.is_terminal_punctuation_udf = udf(self.is_terminal_punctuation, BooleanType())
        self.get_word_count_udf = udf(self.get_word_count, IntegerType())
        self.is_junk_content_udf = udf(self.is_junk_content, BooleanType())
        self.is_numbers_udf = udf(self.is_numbers, BooleanType())
        self.get_stop_word_dist_udf = udf(self.get_stop_word_dist, MapType(StringType(), IntegerType()))
        self.get_stopword_count_udf = udf(self.get_stopword_count, IntegerType())
        self.get_nsfw_word_dist_udf = udf(self.get_nsfw_word_dist, ArrayType(ArrayType(IntegerType())))

    def run_spark_pipeline(self, df, text_column, lid_column):
        df = df.withColumn("is_terminal_punctuation", self.is_terminal_punctuation_udf(text_column))
        df = df.withColumn("word_count", self.get_word_count_udf(df["value"]))
        df = df.withColumn("is_junk_content", self.is_junk_content_udf(df["value"], lit("lang")))
        df = df.withColumn("is_numbers", self.is_numbers_udf(df["value"]))
        df = df.withColumn("stop_word_dist", self.get_stop_word_dist_udf(df["value"], lit("lang")))
        df = df.withColumn("stop_word_count", self.get_stopword_count_udf(df["stop_word_dist"]))
        df = df.withColumn("nsfw_word_dist", self.get_nsfw_word_dist_udf(df["value"], lit("lang")))
        return df