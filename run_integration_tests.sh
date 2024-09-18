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

# update pip version
python3 -m pip install --user --upgrade pip setuptools wheel

# Create test venv
ENV_NAME=".tightlock_integration_test_venv"
python3 -m venv $ENV_NAME

# point PY3 variable to venv python path
PY3=$ENV_NAME/bin/python

$PY3 -m pip install -r integration_tests/test_requirements.txt

# Create env
./create_env.sh non-interactive test 

# Remove potentially running containers and run integration tests
docker compose down 
$PY3 -m pytest -s --no-header -vv --docker-compose=docker-compose.yaml integration_tests/

# get return value of integration tests run
TESTS_RESULT=$?

unset $ENV_NAME

exit $TESTS_RESULT
