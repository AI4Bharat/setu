#!/usr/bin/env bash

# Copyright 2019 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#            http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euxo pipefail

apt-get install -y libprotobuf-dev protobuf-compiler

git clone https://github.com/anoopkunchukuttan/indic_nlp_library.git /opt/indic_nlp_library

export PYTHONPATH=$PYTHONPATH:/opt/indic_nlp_library

pip_requirements=$(/usr/share/google/get_metadata_value attributes/pip-requirements-uri)
echo "Metadata pip-requirements-uri=${pip_requirements}"

temp_config_file=$(mktemp /tmp/pip_env_XXX.txt)
gsutil cp "${pip_requirements}" "${temp_config_file}"

echo "Installing custom packages..."
pip install -r "${temp_config_file}"
echo "Successfully installed custom packages."