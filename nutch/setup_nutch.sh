#!/bin/bash

# wget https://dlcdn.apache.org//ant/binaries/apache-ant-1.10.14-bin.tar.gz .

# tar -xvf apache-ant-1.10.14-bin.tar.gz

# export ANT_HOME=$PWD/apache-ant-1.10.14
# export PATH=$PATH:$ANT_HOME/bin

# rm -rf apache-ant-1.10.14-bin.tar.gz

# wget https://dlcdn.apache.org/nutch/1.19/apache-nutch-1.19-src.tar.gz .

# tar -xvf apache-nutch-1.19-src.tar.gz

# rm -rf apache-nutch-1.19-src.tar.gz

# cd apache-nutch-1.19

# ant

if [[ $1 ]]; then
    gsutil -m cp -r $PWD/apache-nutch-1.19/runtime/deploy $1
    echo "Uploaded nutch deployment module to $1...."
else
    echo "Not uploading nutch deployment module to GCP.... To upload run ``bash setup_nutch.sh gs://<bucket-path>``"
fi

