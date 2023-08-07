# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ENV=".env"
if ! [ -f $ENV ]; then
  # create env file and write Airflow UID ang GID to it
  echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > $ENV

  INTERACTIVE_FLAG=$1
  ENV_FLAG=$2
  PROVIDED_API_KEY=$3

  # create env specific variables
  if [ $ENV_FLAG == "prod" ]; then
    API_PORT=80
  else
    API_PORT=8081
  fi
  echo -e "API_PORT=$API_PORT" >> $ENV

  # get latest git tag and save it to a variable
  echo -e "LATEST_TAG=$(git describe --tags --abbrev=0)" >> $ENV

  # generate or read API key
  PSEUDORANDOM_API_KEY=$( dd bs=512 if=/dev/urandom count=1 2>/dev/null | tr -dc '[:alpha:]' | fold -w20 | head -n 1 )
  if ! [ $INTERACTIVE_FLAG == "non-interactive" ]; then
    echo "Choose API key generation method."
    select yn in "User-provided" "Pseudorandom"; do
        case $yn in
            User-provided ) read -p "Enter API KEY: " API_KEY; break;;
            Pseudorandom ) API_KEY=$PSEUDORANDOM_API_KEY; echo "API key: ${API_KEY}"; break;;
        esac
    done
  else
    if [[ -n "$PROVIDED_API_KEY" ]]; then
      API_KEY=$PROVIDED_API_KEY
    else
      API_KEY=$PSEUDORANDOM_API_KEY
    fi
  fi
  # append API key to env file
  echo -e "TIGHTLOCK_API_KEY=$API_KEY" >> $ENV
fi
