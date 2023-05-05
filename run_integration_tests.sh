# update pip version
python3 -m pip install --user --upgrade pip

# Create test venv
ENV_NAME=".tightlock_integration_test_venv"
python3 -m venv $ENV_NAME

# point PY3 variable to venv python path
PY3=$ENV_NAME/bin/python

$PY3 -m pip install -r integration_tests/test_requirements.txt

# Create env
./create_env.sh "--non-interactive"

# Remove potentially running containers and run integration tests
docker-compose down 
$PY3 -m pytest -s --no-header -vv --docker-compose=docker-compose.yaml integration_tests/

unset $ENV_NAME
