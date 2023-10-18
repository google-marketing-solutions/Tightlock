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

from datetime import datetime
import json
import re
import traceback
from dataclasses import asdict
from typing import Any, Mapping

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import DAG, Variable

from protocols.destination_proto import DestinationProto
from protocols.source_proto import SourceProto
from utils.dag_utils import DagUtils


class DAGBuilder:
  """Builder class for dynamic DAGs."""

  def __init__(self):
    self.latest_config = self._get_latest_config()
    self.register_errors_var = "register_errors"
    Variable.set(key=self.register_errors_var,
                 value=[],
                 description="Report of dynamic DAG registering errors.")

  def _get_latest_config(self):
    sql_stmt = "SELECT value FROM Config ORDER BY create_date DESC LIMIT 1"
    cursor = DagUtils.exec_postgres_command(sql_stmt, (datetime.now(),))
    return cursor.fetchone()[0]

  def _config_from_ref(
      self, ref: Mapping[str, str]) -> SourceProto | DestinationProto:
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
      return DagUtils.import_entity(target_type,
                                    target_folder).Source(target_config)
    elif target_folder == "destinations":
      return (DagUtils.import_entity(target_type,
                                     target_folder).Destination(target_config),
              target_type, target_folder, target_config)
    raise ValueError(f"Not supported folder: {target_folder}")

  def register_dags(self):
    """Loops over all configured connections and create an Airflow DAG for each one of them."""
    # TODO(b/290388517): Remove mentions to activation once UI is ready
    for connection in self.latest_config["activations"]:
      # actual implementations of each source and destination
      try:
        source = self._config_from_ref(connection["source"])
        (destination, dest_type, dest_folder,
         dest_config) = self._config_from_ref(connection["destination"])
        dynamic_dag = DagUtils.build_dynamic_dag(
            connection_name=connection['name'],
            schedule=connection['schedule'],
            target_source=source,
            target_destination=destination,
            dest_type=dest_type,
            dest_folder=dest_folder,
            dest_config=json.dumps(dest_config))
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
