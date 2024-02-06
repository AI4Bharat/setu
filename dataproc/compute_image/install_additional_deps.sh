#!/bin/bash

gsutil -m cp -r gs://sarvam_sangraha/setu_old/dataproc/envs/gcld3* /opt/conda/miniconda3/envs/setu_dataproc/lib/python3.10/site-packages

rm /lib/x86_64-linux-gnu/libstdc++.so.6

cp /opt/conda/miniconda3/envs/setu_dataproc/lib/libstdc++.so.6 /lib/x86_64-linux-gnu

wget https://github.com/anoopkunchukuttan/indic_nlp_library/archive/refs/heads/master.zip
unzip master.zip
mv indic_nlp_library-master/indicnlp /opt/conda/miniconda3/envs/setu_dataproc/lib/python3.10/site-packages

mkdir -p /opt/setu/filter_data
gsutil -m cp -r gs://sarvam_sangraha/setu/setu/data/* /opt/setu/filter_data