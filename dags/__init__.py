"""
 Copyright 2023 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 """

import os
import sys
import time
import uuid

import cloud_detect
import yaml

ROOT_DIR = os.path.dirname(
    os.path.abspath(__file__))  # This is your Project Root
sys.path.append(ROOT_DIR)

TADAU_DIR_PATH = f"{ROOT_DIR}/tadau"
USAGE_DATA_COLLECTION_ALLOWED = os.environ.get(
    "USAGE_COLLECTION_ALLOWED", False)

if USAGE_DATA_COLLECTION_ALLOWED and not os.path.exists(TADAU_DIR_PATH):
  # instantiates Tadau config
  mode = 0o666
  os.makedir(TADAU_DIR_PATH, mode)

  ts = time.time()

  with open(f"{TADAU_DIR_PATH}/config.yaml") as f:
    y = yaml.safe_load(f)
    y["fixed_dimensions"]["deploy_id"] = f"tightlock_{str(uuid.uuid4())}"
    y["fixed_dimensions"]["deploy_infra"] = cloud_detect.provider()
    y["fixed_dimensions"]["deploy_created_time"] = ts
    y["api_secret"] = "4fBQHqNPRT6fRzrSJwucyg"
    y["measurement_id"] = "G-B6RZNC295J"
    y["opt_in"] = USAGE_DATA_COLLECTION_ALLOWED

    yaml.dump(y)


