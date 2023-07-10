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
from asyncio import tasks
from typing import Annotated, Dict, Literal, Optional, Sequence, Union

from airflow.decorators import dag, task
from protocols.destination_proto import DestinationProto
from protocols.source_proto import SourceProto
from utils import DagUtils

start_date = datetime.datetime(2023, 1, 1, 0, 0, 0)
dag_utils = DagUtils()

import importlib
import sys

from pydantic import BaseModel


def _load_schemas_type():
  target = {}
  for folder_name in ("sources", "destinations"):
    class_name = "Source" if folder_name == "sources" else "Destination" if folder_name == "destinations" else None
    if not class_name:
      raise ValueError(f"folder_name '{folder_name}' is not supported.")
    modules = dag_utils.import_modules_from_folder(folder_name)
    module_schemas = []
    for module in modules:
      spec = importlib.util.find_spec(module.__name__)
      print(f"SPEC >>>>>> {spec}")
      print(f"MODULE >>>>>> {module}")
      implementation = getattr(module, class_name)
      if not implementation.schema():
        continue
      print(f"NAME >>>>>>> {implementation.schema().__name__}")
      module_schemas.append(f"{module.__name__}.{implementation.schema().__name__}")
      sys.modules[module.__name__] = module
      spec.loader.exec_module(module)
      exec(f"import {module.__name__}")

    target_type = f"{folder_name}_schemas"
    schemas_str = ' | '.join(module_schemas)
    assign_str = f"target['{folder_name}'] = {schemas_str}"
    exec(assign_str)
    print(f"SCHEMAS >>>>>>> {target.values()}")


  class Schemas(BaseModel):
    sources: target["sources"]
    destinations: target["destinations"]

  return Schemas.schema_json()

@dag(
    dag_id="schema_validation",
    is_paused_upon_creation=False,
    start_date=start_date,
    schedule_interval=None,
    render_template_as_native_obj=True,
    catchup=False
)
def schema_dag():

  @task
  def retrieve_schemas():
    return _load_schemas_type()

  retrieve_schemas()
  
schema_dag()
