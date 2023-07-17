import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
from .commons import *
from matplotlib.font_manager import FontProperties

def plot_line_lengths(lengths):

    wcs = []
    ccs = []
    bcs = []

    for ll in lengths:
        wcs += [ll["wc"]]
        ccs += [ll["cc"]]
        bcs += [ll["bc"]]

    wc_f, _ = hist_plot(wcs, "No.of Word per Line Distribution", "No.of Words", "Line Count")
    cc_f, _ = hist_plot(ccs, "No.of Characters per Line Distribution", "No.of Characters", "Line Count")
    bc_f, _ = hist_plot(bcs, "Size in bytes per Line Distribution", "Size (in bytes)", "Line Count")

    return {
        "wc": wc_f, 
        "cc": cc_f,
        "bc": bc_f
    }

def plot_word_distribution(word_distribution):

    sorted_wd = sorted(word_distribution.items(), key=lambda x:x[1], reverse=True)

    keys = []
    values = []

    for key, value in sorted_wd[:25]:
        keys += [key]
        values += [value]

    wd_f, _ = bar_plot(keys, values, "Word Distribution (Top 25)", "Words", "Count")

    return wd_f

def plot_char_distribution(char_distribution):

    return None

def plot_nsfw_distribution(nsfw_word_distribution):

    return None

def extract_document_level_plots(document_level_stats):

    plots = {}

    plots["ll"] = plot_line_lengths(document_level_stats["line_lengths"])
    plots["word_dist"] = plot_word_distribution(document_level_stats["word_distribution"])
    plots["char_dist"] = plot_char_distribution(document_level_stats["char_distribution"])
    plots["nsfw_dist"] = plot_nsfw_distribution(document_level_stats["nsfw_word_distribution"])

    return plots