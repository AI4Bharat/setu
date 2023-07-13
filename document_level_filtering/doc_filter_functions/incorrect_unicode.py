from ftfy import fix_and_explain

def calc_incorrect_unicode(text):
    _, explain = fix_and_explain(text)

    incorrect_unicodes = 0 if not explain else len(explain)
    return incorrect_unicodes
