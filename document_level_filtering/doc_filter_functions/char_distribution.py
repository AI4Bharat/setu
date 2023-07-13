def calc_char_distribution(d: str):
    cc = dict()

    for char in d:
        c = char.lower().strip()
        cc[c] = cc.get(c, 0) + 1

    return cc
