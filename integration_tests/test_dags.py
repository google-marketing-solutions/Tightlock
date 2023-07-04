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

"""Integration tests for airflow-webserver container."""

import json
from urllib import parse


def test_dag_import_errors(helpers):
  """Verifies if there are no DAG import errors."""
  # TODO(b/278797552): Import errors for dynamic dags are not captured anymore after graceful failures were implemented
  request_session, api_url = helpers.get_airflow_client()
  import_errors = request_session.get(
      parse.urljoin(api_url,
                    "api/v1/variables/register_errors")).json()
  if len(json.loads(import_errors["value"])) > 0:
    for error in import_errors["import_errors"]:
      print(f"Import Error for DAG {error['filename']}: {error['stack_trace']}")
  assert import_errors["total_entries"] == 0
