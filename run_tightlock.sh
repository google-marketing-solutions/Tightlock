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

  # read flags
  ENV_FLAG=dev
  INTERACTIVE_FLAG=interactive
  while getopts ":e:i:" opt; do
  case $opt in
    e) ENV_FLAG=$OPTARG;;
    i) INTERACTIVE_FLAG=$OPTARG;;
    \?) echo "Invalid option -$OPTARG" >&2
    exit 1;;
  esac
done

# Create env
bash ./create_env.sh $INTERACTIVE_FLAG $ENV_FLAG

# define which docker-compose command to use based on the environment
if [ $ENV_FLAG == "prod" ]; then
  COMPOSE_CMD="docker run --name=tightlock -v /var/run/docker.sock:/var/run/docker.sock --rm -v $PWD:$PWD -w $PWD docker/compose:1.29.2"
else
  COMPOSE_CMD='docker-compose'
fi

# Run containers
$COMPOSE_CMD up --build -d
