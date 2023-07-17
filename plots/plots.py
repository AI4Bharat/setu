import matplotlib.pyplot as plt
import seaborn as sns
from .document_level_plots import *
from .line_level_plots import *

def run_plots(document_level_stats, line_level_stats):

    doc_lvl_plots = extract_document_level_plots(document_level_stats)
    line_lvl_plots = extract_line_level_plots(line_level_stats)

    return doc_lvl_plots, line_lvl_plots