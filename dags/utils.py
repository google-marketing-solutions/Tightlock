"""Utility functions for DAGs."""

import importlib
import os
import pathlib
import sys
from dataclasses import dataclass
from typing import Any, List, Mapping, Sequence, Tuple

from airflow.providers.apache.drill.hooks.drill import DrillHook

_TABLE_ALIAS = "t"


@dataclass
class ValidationResult:
  """Class for reporting of validation results."""
  is_valid: bool
  message: str


class DagUtils:
  """A set of utility functions for DAGs."""

  def import_modules_from_folder(self, folder_name: str):
    """Import all modules from a given folder."""
    modules = []
    dags_path = f"airflow/dags/{folder_name}"
    folder_path = pathlib.Path().resolve().parent / dags_path
    for filename in os.listdir(folder_path):
      if os.path.isfile(folder_path / filename) and filename != "__init__.py":
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

  def _parse_data(self, fields, rows: List[Tuple[str, ...]]) -> List[Mapping[str, Any]]:
    """Parses data and transforms it into a list of dictionaries."""
    events = []
    for event in rows:
      event_dict = {}
      # relies on Drill preserving the order of fields provided in the query
      for i, field in enumerate(fields):
        event_dict[field] = event[i]
      events.append(event_dict)
    return events

  def get_drill_data(
      self, from_target: Sequence[str], fields: Sequence[str], offset: int, limit: int
  ) -> List[Mapping[str, Any]]:
    drill_conn = DrillHook().get_conn()
    cursor = drill_conn.cursor()
    table_alias = _TABLE_ALIAS
    fields_str = ",".join([f"{table_alias}.{field}" for field in fields])
    query = (
        f"SELECT {fields_str}"
        f" FROM {from_target} as {table_alias}"
        f" LIMIT {limit} OFFSET {offset}"
    )
    try:
      cursor.execute(query)
      results = self._parse_data(fields, cursor.fetchall())
    except RuntimeError:
      # Return an empty list when an empty cursor is fetched
      results = []
    return results

  def validate_drill(self, path: str) -> ValidationResult:
    drill_conn = DrillHook().get_conn()
    cursor = drill_conn.cursor()
    query = f"SELECT COUNT(1) FROM {path}"
    try:
      cursor.execute(query)
    except Exception:  # pylint: disable=broad-except
      return ValidationResult(False, f"Invalid location: {path}")
    return ValidationResult(True, "")
