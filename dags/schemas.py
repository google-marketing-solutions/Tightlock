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

import datetime
from dataclasses import make_dataclass

from airflow.decorators import dag, task
from pydantic import BaseModel
from utils import DagUtils, ProtocolSchema

start_date = datetime.datetime(2023, 1, 1, 0, 0, 0)
dag_utils = DagUtils()


def reduce_schemas(schemas: list[ProtocolSchema], final_schema=None):
  """Combine schemas into a single Union type."""
  if schemas:
    head = schemas[0]
    tail = schemas[1:]
    if final_schema:
      return reduce_schemas(tail, final_schema | head)
    else:
      return reduce_schemas(tail, head)
  else:
    return final_schema


@dag(
    dag_id="schema_validation",
    is_paused_upon_creation=False,
    start_date=start_date,
    schedule_interval=None,
    render_template_as_native_obj=True,
    catchup=False
)
def schema_dag():
  """DAG that retrieves schemas from every source and destination."""
  @task
  def retrieve_schemas():
    sources_folder = "sources"
    destinations_folder = "destinations"
    module_schemas = {}
    for folder_name in (sources_folder, destinations_folder):
      class_name = "Source" if folder_name == sources_folder else "Destination" if folder_name == destinations_folder else None
      if not class_name:
        raise ValueError(f"folder_name '{folder_name}' is not supported.")
      modules = dag_utils.import_modules_from_folder(folder_name)
      module_schemas[folder_name] = []
      for module in modules:
        implementation = getattr(module, class_name)
        if (schema := implementation.schema()) is None:
          continue  # ignore instances that do not implement schema()
        module_schemas[folder_name].append(
            make_dataclass(schema.class_name, schema.fields))

    Schemas = make_dataclass("Schemas", bases=(BaseModel,), fields=[  # pylint: disable=invalid-name
        ("source", reduce_schemas(module_schemas[sources_folder])),
        ("destination", reduce_schemas(module_schemas[destinations_folder]))
    ])

    return Schemas.schema_json()

  retrieve_schemas()

schema_dag()
