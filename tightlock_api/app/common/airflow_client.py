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
limitations under the License."""

import ast
import datetime
import json
import os
import random
import time
from typing import Any, Optional

import httpx
from models import (Connection, Logs, RunLog, RunLogsResponse, RunResult,
                    ValidationResult)

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
          is_valid=False, messages=[f"Target `{target_name}` is unavailable."]
      )
    parsed_xcom_response = json.loads(xcom_response.content)
    # Parse json with literal_eval as XCOM returns the response with single quotes
    validation_result = ast.literal_eval(parsed_xcom_response["value"])
    return ValidationResult(**validation_result)

  async def _get_dag_run_xcom(self, dag_id: str, dag_run_id: str, xcom_key: str):
    task_id = dag_id  # currently, dags only have one task and share id with tasks
    url = f"{self.base_url}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}"
    response = await self._get_request(
        url, status_forcelist=[]
    )  # do not retry to improve performance
    return response

  def _build_run_log_response(
      self, connection: Connection, run: dict[str, Any], run_result: RunResult
  ) -> RunLog:
    default_str_value = "Missing"
    source_name = connection.source["$ref"].split("#/sources/")[1]
    destination_name = connection.destination["$ref"].split("#/destinations/")[1]
    run_log = RunLog(
        connection_name=connection.name,
        source_name=source_name or default_str_value,
        destination_name=destination_name or default_str_value,
        schedule=connection.schedule or "None",
        state=run.get("state") or default_str_value,
        run_at=run.get("end_date"),
        run_type=run.get("run_type") or default_str_value,
        run_result=run_result,
    )

    return run_log

  async def list_dag_runs(
      self,
      connection_by_dag_id: dict[str, Connection],
      offset: int = 0,
      limit: int = 50,
  ) -> RunLogsResponse:
    dag_ids = [dag_id for dag_id in connection_by_dag_id.keys()]
    order_by = "-execution_date"
    url = f"{self.base_url}/dags/~/dagRuns/list"
    payload = {
        "dag_ids": dag_ids,
        "order_by": order_by,
        "page_offset": offset,
        "page_limit": limit,
    }
    list_response = await self._post_request(url, payload)
    list_response_json = list_response.json()
    runs = list_response_json.get("dag_runs")
    total_entries = list_response_json.get("total_entries")
    run_logs = []
    for run in runs:
      dag_id = run["dag_id"]
      dag_run_id = run["dag_run_id"]
      xcom_key = "run_result"
      run_result_response = await self._get_dag_run_xcom(dag_id, dag_run_id, xcom_key)
      run_result_json = run_result_response.json()
      run_result = ast.literal_eval(run_result_json.get("value") or '{}')
      run_log = self._build_run_log_response(
          connection_by_dag_id[dag_id], run, RunResult(**run_result)
      )
      run_logs.append(run_log)

    response = RunLogsResponse(
      run_logs=run_logs,
      total_entries=total_entries
    )

    return response

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

  async def get_schemas(self) -> Optional[str]:
    # Trigger schema DAG
    dag_id = "retrieve_schemas"
    task_id = dag_id  # this task has the same name as the dag
    trigger_result = await self.trigger(dag_id, "")
    content = json.loads(trigger_result.content)

    # Get result of schemas DAG
    dag_run_id = content["dag_run_id"]
    url = f"{self.base_url}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/return_value"
    xcom_response = await self._get_request(url)
    if xcom_response.status_code != 200:
      return None
    parsed_xcom_response = json.loads(xcom_response.content)
    schemas_result = parsed_xcom_response["value"]
    return schemas_result

  async def get_latest_logs(self):
    print("Trying to get logs...")
    path = "/app/logs/"

    log_dir = os.listdir(path)

    dag_log_dirs = [folder for folder in log_dir if folder.endswith("_dag")]

    print(f"Dag log dirs: {dag_log_dirs}")

    latest_log_subdirs = []

    # Gets latest subdir (e.g. 'logs/dag_id=ga4_web_bq_dag/run_id=manual__2023-09-20T14:29:24+00:00)
    for dag_log_dir in dag_log_dirs:
        subdir_path = os.path.join(path, dag_log_dir)
        run_ids = os.listdir(subdir_path)
        run_ids.sort()
        run_id_path = os.path.join(path, dag_log_dir, run_ids[-1])

        # Get the task_id subfolder
        task_subfolders = os.listdir(run_id_path)

        for task_subfolder in task_subfolders:
            if not task_subfolder.startswith("task_id="):
                continue

            full_path = os.path.join(run_id_path, task_subfolder)

            latest_log_subdirs.append(full_path)

    print(f"Latest subdirs: {latest_log_subdirs}")

    latest_log_files = []

    # Gets latest files within the subdir
    for latest_log_subdir in latest_log_subdirs:
        log_files = os.listdir(latest_log_subdir)
        log_files.sort()
        latest_log_files.append(os.path.join(latest_log_subdir, log_files[-1]))

    print(f"Latest log files: {latest_log_files}")

    log_data = {}

    for log_file in latest_log_files:
        with open(log_file, "r") as f:
            dag_name = log_file.split("/")[3]
            dag_name = dag_name.replace("dag_id=", "")
            print(f"Log file: {dag_name}")
            log_data[dag_name] = f.read()

    response = Logs(logs=log_data)

    return response
