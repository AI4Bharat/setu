import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.font_manager import FontProperties

def bar_plot(x, y, title, xlabel, ylabel):

    font_prop = FontProperties(fname='Mangal.ttf', size=18)

    f, ax = plt.subplots()
    ax.bar(x, y)
    ax.set_title(title)
    ax.set_xlabel(xlabel, fontproperties=font_prop)
    ax.set_ylabel(ylabel)
    return f, ax

# def hist_plot(x, bins, title, xlabel, ylabel):
def hist_plot(x, title, xlabel, ylabel):

    f, ax = plt.subplots()
    # ax.hist(x, bins=bins)
    ax.hist(x)
    ax.set_title(title)
    # ax.set_xlabel(xlabel)
    # ax.set_ylabel(ylabel)
    return f, ax