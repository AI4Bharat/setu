#!/usr/bin/env bash

set -euxo pipefail

CONDA_DEFAULT_ENV=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/attributes/conda-default-env")

if [[ -z "${CONDA_DEFAULT_ENV}" ]]; then
    echo "Expected metadata conda-default-env not found"
    exit 1
fi
echo "CONDA_DEFAULT_ENV=${CONDA_DEFAULT_ENV}" >> /etc/environment

CONDA_PREFIX=/opt/conda/miniconda3/envs/$CONDA_DEFAULT_ENV
echo "CONDA_PREFIX=${CONDA_PREFIX}" >> /etc/environment

CONDA_PROMPT_MODIFIER=
echo "CONDA_PROMPT_MODIFIER=${CONDA_PROMPT_MODIFIER}" >> /etc/environment

if [[ -z "$PATH" ]]; then
    echo "PATH variable not present, adding the entire path"
    PATH=/opt/conda/miniconda3/envs/$CONDA_DEFAULT_ENV/bin:/opt/conda/default/bin:/opt/conda/miniconda3/condabin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/snap/bin
else
    echo "PATH variable already present, adding the conda environment path to the beginning"
    echo "$PATH"
    PATH=/opt/conda/miniconda3/envs/$CONDA_DEFAULT_ENV/bin:$PATH
fi
echo "PATH=${PATH}" >> /etc/environment

git clone https://github.com/anoopkunchukuttan/indic_nlp_library.git /opt/indic_nlp_library

if [[ -z "$PYTHONPATH" ]]; then
    echo "PYTHON_PATH variable not present, adding the entire path"
    PYTHONPATH=/opt/indic_nlp_library
else
    echo "PYTHON_PATH variable already present, adding the indic-nlp-library path at the end"
    echo "$PYTHON_PATH"
    PYTHONPATH=$PYTHONPATH:/opt/indic_nlp_library
fi
echo "PYTHONPATH=${PYTHONPATH}" >> /etc/environment