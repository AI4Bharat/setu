# Setu: Web, PDF, and Speech Data Cleaning Pipeline

Setu is a comprehensive pipeline designed to clean, filter, and deduplicate diverse data sources including Web, PDF, and Speech data. Built on Apache Spark, Setu encompasses four key stages: document preparation, document cleaning and analysis, flagging and filtering, and deduplication.

# Setu Pipeline Stagewise Summary

## Document Preparation

The first stage of Setu focuses on extracting text from a variety of sources to create text documents for further processing. For Web documents, Setu utilizes trafilatura (Barbaresi, 2021b) to extract text from HTML. Meanwhile, PDFs undergo a pipeline that leverages bounding box related information to filter out pages potentially afflicted with recognition issues and noise.

This documentation provides an overview of Setu and its workflow, enabling users to efficiently manage and process Web, PDF, and Speech data with Apache Spark.

## Cleaning and Analysis Stage

In the cleaning and analysis stage, Setu focuses on reducing noise within individual documents. It employs a multi-model approach for language identification, leveraging outputs from IndicLID, NLLB, and gcld3. Various statistics such as character and word counts, NSFW word count, and n-gram repetition ratio are computed during analysis.

## Flagging and Filtering Stage

During the flagging and filtering stage, Setu applies filters based on the computed statistics. Filters include line length filters, NSFW word filters, and repetition filters, aimed at removing noisy and toxic documents.

## Deduplication Stage

The deduplication stage of Setu performs fuzzy deduplication using MinHashLSH implemented in text-dedup. This stage helps in identifying and eliminating duplicate documents, enhancing data cleanliness and efficiency.

# Setu Conda Environment Setup

To set up the Setu environment using Conda and install packages from the `setu_env.txt` file, follow these instructions:

## 1. Install Conda

If you haven't already installed Conda, download and install Miniconda or Anaconda from the [official website](https://docs.conda.io/en/latest/miniconda.html).

## 2. Create a New Conda Environment

Open a terminal or command prompt and create a new Conda environment using the following command:

```console
conda create --name setu_env python=3.10
```

## 3. Activate the Conda Environment

Activate the newly created environment using the following command:

```console
conda activate setu_env
```

## 4. Install Packages from the setu_env.txt File

Use the following command to install packages listed in the `setu_env.txt` file:

```console
conda install --file setu_env.txt
```

This command will install all the required packages specified in the `setu_env.txt` file into your Conda environment.

## 5. Verify Installation

To verify that the packages have been installed correctly, you can run:

```console
conda list
```
