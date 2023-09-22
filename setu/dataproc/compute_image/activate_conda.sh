#!/bin/bash

# Source the conda initialization script to make the conda command available
source /opt/conda/etc/profile.d/conda.sh

# Activate the desired environment for all users
echo "source activate setu_dataproc" >> /etc/profile.d/conda_config.sh
