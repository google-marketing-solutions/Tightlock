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

"""API clients used by TIghtlock routes."""
import ast
import datetime
import json
import random
import time
from typing import Any, Optional

import httpx
from models import ValidationResult

_AIRFLOW_BASE_URL = "http://airflow-webserver:8080"


class AirflowClient:
  """Defines a base airflow client."""

  def __init__(self):
    self.base_url = f"{_AIRFLOW_BASE_URL}/api/v1"
    # TODO(b/267772197): Add functionality to store usn:password.
    self.auth = ("airflow", "airflow")

  async def _post_request(self, url: str, body: dict[str, Any]):
    async with httpx.AsyncClient() as client:
      return await client.post(url, json=body, auth=self.auth)

  async def _get_request(
      self, url: str, status_forcelist=[404], max_retries=3, backoff_in_seconds=1
  ):
    async with httpx.AsyncClient() as client:
      response = await client.get(url, auth=self.auth)
      retries_left = max_retries
      while retries_left > 0 and response.status_code in status_forcelist:
        retries_tried = max_retries - retries_left
        sleep = backoff_in_seconds * 2**retries_tried + random.uniform(0, 1)
        time.sleep(sleep)
        response = await client.get(url, auth=self.auth)
        retries_left -= 1
      return response

  async def _validate_target(
      self, target_class: str, target_name: str, target_config: dict[str, Any]
  ) -> ValidationResult:
    # Trigger validate_source DAG
    conf = {"target_name": target_name, "target_config": target_config}
    dag_id = f"validate_{target_class.lower()}"
    task_id = dag_id  # this task has the same name as the dag
    trigger_result = await self.trigger(dag_id, "", conf)
    content = json.loads(trigger_result.content)

    # Get result of validation
    dag_run_id = content["dag_run_id"]
    url = f"{self.base_url}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/return_value"
    xcom_response = await self._get_request(url)
    if xcom_response.status_code != 200:
      return ValidationResult(
          is_valid=False, messages=f"Target `{target_name}` is unavailable."
      )
    parsed_xcom_response = json.loads(xcom_response.content)
    # Parse json with literal_eval as XCOM returns the response with single quotes
    validation_result = ast.literal_eval(parsed_xcom_response["value"])
    return ValidationResult(**validation_result)

  async def trigger(
      self,
      dag_prefix: str,
      dag_suffix: str = "_dag",
      conf: Optional[dict[str, Any]] = None,
  ):
    now_date = str(datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
    body = {
        "logical_date": now_date,
        "conf": conf or {},
    }
    url = f"{self.base_url}/dags/{dag_prefix}{dag_suffix}/dagRuns"
    return await self._post_request(url, body)

  async def validate_source(
      self, source_name: str, source_config: dict[str, Any]
  ) -> ValidationResult:
    return await self._validate_target("Source", source_name, source_config)

  async def validate_destination(
      self, destination_name: str, destination_config: dict[str, Any]
  ) -> ValidationResult:
    return await self._validate_target(
        "Destination", destination_name, destination_config
    )
