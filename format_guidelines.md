
# FORMAT GUIDELINES

```bash
                                  Dataset-Type
                                /       |       \
                   Source -> Crawl     PDFs     Videos
                              /         |           \
                Folder -> Website      ---      Channel/Domain
                            /           |              \
             Document -> Webpage      1 PDF           Video
                          /             |                 \
          Paragraph -> ----          paragraph           ----
                        /               |                   \
              Line -> Line            Line                 Line
```
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## SOURCE LEVEL FORMAT

```python
{
    "document_size_distribution": List,
    "kenlm_perplexity_score_distribution": Dict[str, float],

    Yet to decide more features for this stage.

}
```

- 1 jsonl will contain jsons of all the sources.
- Naming Convention:
        - Name: `<source-name>_<dataset-type>.jsonl`
        - Eg: sangraha_crawl.jsonl, sangraha_pdf.jsonl, pile_crawl.jsonl etc

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## FOLDER LEVEL FORMAT

```python
{
    Yet to decide which features to extract at this stage.
}
```

- 1 jsonl will contain jsons of all folders for a given source.
- Naming Convention:
    - Suppose for crawl, we are crawling `AajTak"` website.
    - Name: `<source-name>_<dataset-type>_<website_name>.jsonl`
    - Here, for `AajTak`, name will be: sangraha_crawl_aajtak.jsonl

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## DOCUMENT LEVEL FORMAT

```python
{   
    "identifier": int/str,                                                      # A random set of numbers. Yet to decide how to implement identifiers.
    "source": str,                                                              # URL of source data.
    "text": List[str],                                                          # A list of lines present in the document. Line is defined as a sequence terminating with either a "\n" or terminal punctuation.
    "word_distribution": Dict[str, int],                                        # Occurence frequency of each word in the document.
    "line_lengths": List[int],                                                  # In characters, words or bytes?
    "language_id": {
        "indiclid": Tuple(str, float),                                          # Tuple of language detected and its confidence by IndicLID.
        "cld3": Tuple(str, float),                                              # Tuple of language detected and its confidence by cld3.
        "nllb": Tuple(str, float),                                              # Tuple of language detected and its confidence by NLLB.
    },
    "num_lines": int,                                                           # No. of lines in the document.
    "num_of_bad_unicodes": int,                                                 # Optional - No.of bad unicode characters present in the dataset.
    "char_distribution": Dict[str, int],                                        # Occurence frequency of each character in the document.
    "kenlm_perplexity_score": Dict[str, float],                                 # perplexity scores from multiple LMs.
    "nsfw_word_distribution": Dict[str, int],                                   # Occurence frequency of each NSFW words in the document.

    "code_stats": [{                                                            # List[Dict] - List of code spans present in the document along with some features of each code span.

        "start": int,                                                           # start index of the code span detected by regex.
        "end": int,                                                             # end index of the code span detected by regex.
        "num_of_chars_of_code": int,                                            # no.of chars present in the code span.
        "words_distribution": Dict[str, int],                                   # Occurence frequency of each word in the code span.

    }],

    "repeated_line_distribution": Dict[str, int],                               # Occurence frequency of each line in the document.
    "repeated_ngram_distribution": Dict[str, Dict[str, int]],                   # Occurence frequency of each ngram in the document.
}
```

- 1 jsonl will contain jsons of all the documents for a given folder.
- Naming Convention:
    - Suppose, in sangraha, in `AajTak` website, we are at blog `123134123`
    - Name: `<source-name>_<dataset-type>_<website_name>_<blog_id>.jsonl`
    - Here, name will be: sangraha_crawl_aajtak_123134123.jsonl

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# LINE LEVEL FORMAT

```python
{
    "identifier": int/str,                                                      # A random set of numbers. Yet to decide how to implement identifiers.
    "term_valid": Bool,                                                         # Whether the line is ending with a terminal punctuation.
    "is_junk": Bool,                                                            # Whether the line is junk or not.
    "is_num": Bool,                                                             # Whether the line contains only numbers or not.
    "stop_word_distribution": Dict[str, int],                                   # A Dictionary of stop words and their occurence frequency.
    "language_id": {
        "indiclid": Tuple(str, float),                                          # Tuple of language detected and its confidence by IndicLID.
        "cld3": Tuple(str, float),                                              # Tuple of language detected and its confidence by IndicLID.
        "nllb": Tuple(str, float),                                              # Tuple of language detected and its confidence by IndicLID.
    },
    "nsfw_span": List[Tuple(int, int)],                                         # A list of tuples each containing the start and end indices of each NSFW span.
}
```

- 1 jsonl will contain jsons of all the lines for a given document.
- Naming Convention:
    - Suppose, in sangraha, in aajtak website, in blog `123134123`, we are at line 10
    - Name: `<source-name>_<dataset-type>_<website_name>_<blog_id>_lines.jsonl`
    - Here, the name will be: sangraha_crawl_aajtak_123134123_lines.jsonl
    - The json at 10th place should contain line-level stats of line 10 in the document.