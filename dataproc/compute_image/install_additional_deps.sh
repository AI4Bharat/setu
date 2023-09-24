#!/bin/bash

gsutil -m cp -r gs://sangraha/setu/dataproc/envs/gcld3* /opt/conda/miniconda3/envs/setu_dataproc/lib/python3.10/site-packages

wget https://github.com/anoopkunchukuttan/indic_nlp_library/archive/refs/heads/master.zip
unzip master.zip
mv indic_nlp_library-master/indicnlp /opt/conda/miniconda3/envs/setu_dataproc/lib/python3.10/site-packages