#!/bin/usr/env bash

mkdir -p /data/priyam/sangraha/spark_out/bengali/dataset/cleaned_docs
gsutil -m cp -r gs://sangraha/spark_out/bengali/dataset/cleaned_docs/symbol_heavy /data/priyam/sangraha/spark_out/bengali/dataset/cleaned_docs

