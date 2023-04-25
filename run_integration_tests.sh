# update pip version
python3 -m pip install --user --upgrade pip

# Create test venv
ENV_NAME=".tightlock_integration_test_venv"
python3 -m venv $ENV_NAME

# point PY3 variable to venv python path
PY3=$ENV_NAME/bin/python

$PY3 -m pip install -r integration_tests/test_requirements.txt

$PY3 -m pytest -r -o log_cli=true --no-header -vv --docker-compose=docker-compose.yaml --docker-compose-no-build --use-running-containers  integration_tests/
