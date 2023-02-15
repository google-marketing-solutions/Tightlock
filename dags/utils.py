"""Utility functions for DAGs."""

import importlib
import os
import pathlib
import sys

from typing import Any, Iterable, Sequence

from airflow.providers.apache.drill.hooks.drill import DrillHook


class DagUtils:
  """A set of utility functions for DAGs."""

  def import_modules_from_folder(self, folder_name: str):
    """Import all modules from a given folder."""
    modules = []
    dags_path = f"airflow/dags/{folder_name}"
    folder_path = pathlib.Path().resolve().parent / dags_path
    for filename in os.listdir(folder_path):
      if os.path.isfile(filename):
        module_name, _ = filename.split(".py")
        module_path = os.path.join(folder_path, filename)
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        modules.append(module)
    return modules


class DrillMixin:
  """A Drill mixin that provides a get_drill_data wrapper for other classes that use drill."""

  def get_drill_data(self,
                     from_target: Sequence[str],
                     fields: Sequence[str]) -> Iterable[Any]:
    drill_conn = DrillHook().get_conn()
    cursor = drill_conn.cursor()
    fields_str = ",".join(fields)
    query = f"SELECT {fields_str} FROM {from_target}"
    cursor.execute(query)
    return cursor
