from indicnlp.tokenize.indic_tokenize import trivial_tokenize
from indicnlp.tokenize.sentence_tokenize import sentence_split


# length in words and characters and bytes
def calc_line_lengths(d: str, lang_code: str):
    ll = [
        {
            "wc": len(trivial_tokenize(line, lang_code)),  # word count
            "cc": len(line),  # character count
            "bc": len(str.encode(line)),  # byte count
        }
        for line in sentence_split(d, lang_code)
    ]

    return ll
