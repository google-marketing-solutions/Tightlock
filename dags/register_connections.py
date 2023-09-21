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

"""Registers connections dynamically from config."""

import ast
import datetime
import importlib.util
import pathlib
import re
import traceback
from dataclasses import asdict
from functools import partial
from typing import Any, Mapping, Optional, Sequence

from airflow.decorators import dag
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from protocols.destination_proto import DestinationProto
from protocols.transformation_proto import TransformationProto
from protocols.source_proto import SourceProto
from utils import RunResult


class DAGBuilder:
  """Builder class for dynamic DAGs."""

  def __init__(self):
    self.latest_config = self._get_latest_config()
    self.register_errors_var = "register_errors"
    Variable.set(key=self.register_errors_var,
                 value=[],
                 description="Report of dynamic DAG registering errors.")

  def _config_from_ref(
      self, ref: Mapping[str, str]
  ) -> SourceProto | DestinationProto | TransformationProto:
    refs_regex = r"^#\/(sources|destinations|transformations)\/(.*)"
    ref_str = ref["$ref"]
    match = re.search(refs_regex, ref_str)
    target_folder = match.group(1)
    target_name = match.group(2)
    target_config = self.latest_config[target_folder][target_name]
    target_type = target_config.get("type")
    if not target_type:
      raise ValueError("Missing config attribute `type`.")

    entity = self._import_entity(target_type, target_folder)
    if target_folder == "sources":
      return entity.Source(target_config)
    elif target_folder == "transformations":
      return entity.Transformation(target_config)
    elif target_folder == "destinations":
      return entity.Destination(target_config)

    raise ValueError(f"Not supported folder: {target_folder}")

  def _configs_from_refs(
      self, refs: Sequence[Mapping[str, str]]
  ) -> Sequence[TransformationProto]:
    return [self._config_from_ref(ref) for ref in refs]

  def _parse_dry_run(self, connection_id: str, dry_run_str: str) -> bool:
    try:
      dry_run = ast.literal_eval(dry_run_str)
      if dry_run:
        print(f"Dry-run enabled for {connection_id}")
      return dry_run
    except ValueError:
      print(f"Dry-run defaulting to False for {connection_id}")
      return False

  def _import_entity(
      self, source_name: str, folder_name: str
  ) -> SourceProto | DestinationProto | TransformationProto:
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
      connection: Mapping[str, Any],
      target_source: SourceProto,
      target_transformations: Sequence[TransformationProto],
      target_destination: DestinationProto,
      reusable_credentials: Optional[Sequence[Any]] = None,
  ):
    """Dynamically creates a DAG based on a given connection."""
    connection_id = f"{connection['name']}_dag"

    schedule = connection["schedule"]
    schedule_interval = schedule if schedule else None

    start_date = datetime.datetime(2023, 1, 1, 0, 0, 0)

    @dag(
        dag_id=connection_id,
        is_paused_upon_creation=False,
        start_date=start_date,
        schedule_interval=schedule_interval,
        catchup=False
    )
    def dynamic_generated_dag():
      def process(task_instance, dry_run_str: str) -> None:
        dry_run = self._parse_dry_run(connection_id, dry_run_str)
        fields = target_destination.fields()
        batch_size = target_destination.batch_size()
        offset = 0

        for transformation in target_transformations:
          fields = transformation.pre_transform(fields)

        get_data = partial(
            target_source.get_data,
            fields=fields,
            limit=batch_size,
            reusable_credentials=reusable_credentials
        )
        run_result = RunResult(0, 0, [], dry_run)
        while True:
          data = get_data(offset=offset)
          if not data:
            break
          for transformation in target_transformations:
            data = transformation.post_transform(data)
          run_result += target_destination.send_data(data, dry_run)
          offset += batch_size

        task_instance.xcom_push("run_result", asdict(run_result))

      PythonOperator(
          task_id=connection_id,
          op_kwargs={"dry_run_str": "{{dag_run.conf.get('dry_run', False)}}"},
          python_callable=process,
      )

    return dynamic_generated_dag

  def register_dags(self):
    """Loops over all configured connections and create an Airflow DAG for each one of them."""
    # TODO(b/290388517): Remove mentions to activation once UI is ready
    for connection in self.latest_config["activations"]:
      # actual implementations of each source, transformation, and destination
      try:
        target_source = self._config_from_ref(connection["source"])
        target_transformations = []
        if 'transformations' in connection:
          target_transformations = self._configs_from_refs(connection['transformations'])
        target_destination = self._config_from_ref(connection["destination"])
        dynamic_dag = self._build_dynamic_dag(
            connection, target_source, target_transformations, target_destination
        )
        # register dag by calling the dag object
        dynamic_dag()
      except Exception:  # pylint: disable=broad-except
        error_traceback = traceback.format_exc()
        register_errors = Variable.get(self.register_errors_var,
                                       deserialize_json=True)
        register_errors.append({
            "connection_name": connection["name"],
            "error": error_traceback
        })
        print(f"{connection['name']} registration error : {error_traceback}")

        Variable.update(self.register_errors_var,
                        register_errors,
                        serialize_json=True)


builder = DAGBuilder()
builder.register_dags()
