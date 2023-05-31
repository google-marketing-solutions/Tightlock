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

"""Registers activations dynamically from config."""

import ast
import datetime
import importlib.util
import pathlib
import re
import traceback
from dataclasses import asdict
from functools import partial
from typing import Any, Mapping, Sequence

from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from protocols.destination_proto import DestinationProto
from protocols.source_proto import SourceProto
from utils import RunResult


class DAGBuilder:
  """Builder class for dynamic DAGs."""

  def __init__(self):
    self.latest_config = self._get_latest_config()

  def _config_from_ref(self, ref: Mapping[str, str]) -> SourceProto | DestinationProto:
    refs_regex = r"^#\/(sources|destinations)\/(.*)"
    ref_str = ref["$ref"]
    match = re.search(refs_regex, ref_str)
    target_folder = match.group(1)
    target_name = match.group(2)
    target_config = self.latest_config[target_folder][target_name]
    target_type = target_config.get("type")
    if not target_type:
      raise ValueError("Missing config attribute `type`.")
    if target_folder == "sources":
      return self._import_entity(target_type, target_folder).Source(target_config)
    elif target_folder == "destinations":
      return self._import_entity(target_type, target_folder).Destination(target_config)
    raise ValueError(f"Not supported folder: {target_folder}")

  def _parse_dry_run(self, activation_id: str, dry_run_str: str) -> bool:
    try:
      dry_run = ast.literal_eval(dry_run_str)
      if dry_run:
        print(f"Dry-run enabled for {activation_id}")
      return dry_run
    except ValueError:
      print(f"Dry-run defaulting to False for {activation_id}")
      return False

  def _import_entity(
      self, source_name: str, folder_name: str
  ) -> SourceProto | DestinationProto:
    module_name = "".join(x.title() for x in source_name.split("_") if not x.isspace())

    lower_source_name = source_name.lower()
    filepath = pathlib.Path(f"dags/{folder_name}") / f"{lower_source_name}.py"
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

  def _get_latest_config(self):
    sql_stmt = "SELECT value FROM Config ORDER BY create_date DESC LIMIT 1"
    pg_hook = PostgresHook(
        postgres_conn_id="tightlock_config",
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchone()[0]

  def _build_dynamic_dag(
      self,
      activation: Mapping[str, Any],
      external_connections: Sequence[Any],
      target_source: SourceProto,
      target_destination: DestinationProto,
  ):
    """Dynamically creates a DAG based on a given activation."""
    activation_id = f"{activation['name']}_dag"

    schedule = activation["schedule"]
    schedule_interval = schedule if schedule else None

    @dag(
        dag_id=activation_id,
        is_paused_upon_creation=False,
        start_date=datetime.datetime.now(),
        schedule_interval=schedule_interval,
    )
    def dynamic_generated_dag():
      def process(task_instance, dry_run_str: str) -> None:
        dry_run = self._parse_dry_run(activation_id, dry_run_str)
        fields = target_destination.fields()
        batch_size = target_destination.batch_size()
        offset = 0
        get_data = partial(
            target_source.get_data,
            connections=external_connections,
            fields=fields,
            limit=batch_size,
        )
        data = get_data(offset=offset)
        run_result = RunResult(0, 0, [], dry_run)
        while data:
          run_result += target_destination.send_data(data, dry_run)
          offset += batch_size
          data = get_data(offset=offset)

        task_instance.xcom_push("run_result", asdict(run_result))

      PythonOperator(
          task_id=activation_id,
          op_kwargs={"dry_run_str": "{{dag_run.conf.get('dry_run', False)}}"},
          python_callable=process,
      )

    return dynamic_generated_dag

  def register_dags(self):
    """Loops over all configured activations and create an Airflow DAG for each one of them."""
    external_connections = (
        {}
    )  # TODO(b/277966895): Delete external_connections references if this is not used anymore in the config.

    for activation in self.latest_config["activations"]:
      # actual implementations of each source and destination
      try:
        target_source = self._config_from_ref(activation["source"])
        target_destination = self._config_from_ref(activation["destination"])
        dynamic_dag = self._build_dynamic_dag(
            activation, external_connections, target_source, target_destination
        )
        # register dag by calling the dag object
        dynamic_dag()
      except Exception:  # pylint: disable=broad-except
        print(f"DAG registration error: {traceback.format_exc()}")


builder = DAGBuilder()
builder.register_dags()
