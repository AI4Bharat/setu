import argparse
import os
import re
from concurrent import futures


def parse_args():
    pass


def has_code(text):
    # List of common programming keywords
    programming_keywords = ["for", "while", "if", "else", "class", "function", "def", "return", "import", "include"]

    # Regular expression to detect code-like patterns
    # Example: 'for i in range(10):', 'int a = 5;', 'print("Hello, world!")'
    code_pattern = re.compile(r'\b(?:for|if|while|def|class|return|import|include)\b|[{};()]|\w+\(.*\)|=\s*\S+')

    # Regular expression to detect programming comments
    # Example: '// This is a comment', '/* comment */', '# comment'
    comment_pattern = re.compile(r'//.*|/\*.*\*/|#.*')

    # Detect if text contains programming keywords
    keyword_count = 0
    for keyword in programming_keywords:
        keyword_count += text.count(keyword)
        if keyword_count >= 3:
            keyword_present = True
            break
    else:
        keyword_present = False

    # Detect if text contains code patterns
    code_matches = re.finditer(code_pattern, text)
    code_indices = [(match.start(), match.end()) for match in code_matches]

    # Detect if text contains comments
    comment_matches = re.finditer(comment_pattern, text)
    comment_indices = [(match.start(), match.end()) for match in comment_matches]

    if code_indices and comment_indices:
        return "code_and_comment", code_indices, comment_indices
    elif code_indices:
        return "code", code_indices, None
    elif comment_indices:
        return "comment", None, comment_indices
    elif keyword_present:
        return "keyword", None, None
    else:
        return None, None, None

def process_text(text):
    result = has_code(text)
    return result

def parallel_code_check(text_list):
    with futures.ThreadPoolExecutor() as executor:
        results = executor.map(process_text, text_list)
    return results

def print_results(text_list, results):
    # Process the results
    for text, result in zip(text_list, results):
        content_type, code_indices, comment_indices = result
        print("Text:", text)
        if content_type == "code":
            print("Code found at indices:", code_indices)
        elif content_type == "comment":
            print("Comments found at indices:", comment_indices)
        elif content_type == "code_and_comment":
            print("Code found at indices:", code_indices)
            print("Comments found at indices:", comment_indices)
        elif content_type == "keyword":
            print("Programming keywords found.")
        else:
            print("No code, comments, or keywords found.")
        print("---")

if __name__ == "__main__":

    args = parse_args()

    # text = """
    # How are you? I hope you are fine. There is a dragon arriving today. I don't think that is the right option.
    # import argparse
    # import os
    # def parse_args():
    #     pass    
    # How are you? I hope you are fine. There is a dragon arriving today. I don't think that is the right option.
    # """

    text = """

    How are you? I hope you are fine. There is a dragon arriving today. I don't think that is the right option.

    import argparse
    import os

    def parse_args():
        pass

    def repetition_filter(text):

        return None

    if __name__ == "__main__":

        args = parse_args()

        text = ""

        results = repetition_filter(text)

        print(results)
    
    How are you? I hope you are fine. There is a dragon arriving today. I don't think that is the right option.

    """

    results = process_text(text)

    # results = parallel_code_check([text])

    # print_results([text], results)    
    print(results)
    for start, end in results[1]:
        print(text[start:end])