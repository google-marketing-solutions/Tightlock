"""Utility functions for DAGs."""

import importlib
import os
import pathlib
import sys


class DagUtils:
  """DagUtils class."""

  def import_modules_from_folder(self, folder_name: str):
    modules = []
    folder_path = pathlib.Path().resolve().parent / f"airflow/dags/{folder_name}"
    for filename in os.listdir(folder_path):
      module_name, _ = filename.split(".py")
      module_path = os.path.join(folder_path, filename)
      spec = importlib.util.spec_from_file_location(module_name, module_path)
      module = importlib.util.module_from_spec(spec)
      sys.modules[module_name] = module
      spec.loader.exec_module(module) 
      modules.append(module)
    return modules

