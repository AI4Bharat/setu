import regex

def has_non_latin_indic_chars(text):
    # non_li_pattern = r'[^\p{Script=Latin}\p{Script=Devanagari}\p{Script=Bengali}\p{Script=Gujarati}\p{Script=Gurmukhi}\p{Script=Kannada}\p{Script=Malayalam}\p{Script=Oriya}\p{Script=Tamil}\p{Script=Telugu}\p{Script=Meetei Mayek}\p{Script=Arabic}\p{Script=Dogra}\p{Script=Ol Chiki}\p{P}\s]'
    non_li_pattern = (
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
    print(non_li_pattern)
    non_latin_indic = regex.findall(non_li_pattern, text)
    return len(non_latin_indic) > 0

text = "This is some text. यह कुछ पाठ है. これはテキストです。"
print(has_non_latin_indic_chars(text))  # This will print: True

text = "This is some text. यह कुछ पाठ है."
print(has_non_latin_indic_chars(text))  # This will print: False