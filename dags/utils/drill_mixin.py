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
"""Utility functions for DAGs."""

import traceback
from typing import Any, List, Mapping, Sequence, Tuple

from airflow.providers.apache.drill.hooks.drill import DrillHook

from utils.validation_result import ValidationResult

_TABLE_ALIAS = "t"


class DrillMixin:
  """A Drill mixin that provides a get_drill_data wrapper for other classes that use drill."""

  def _parse_data(self, fields,
                  rows: List[Tuple[str, ...]]) -> List[Mapping[str, Any]]:
    """Parses data and transforms it into a list of dictionaries."""
    events = []
    for event in rows:
      event_dict = {}
      # relies on Drill preserving the order of fields provided in the query
      for i, field in enumerate(fields):
        event_dict[field] = event[i]
      events.append(event_dict)
    return events

  def get_drill_data(self, from_target: Sequence[str], fields: Sequence[str],
                     offset: int, limit: int) -> List[Mapping[str, Any]]:
    drill_conn = DrillHook().get_conn()
    cursor = drill_conn.cursor()
    table_alias = _TABLE_ALIAS
    fields_str = ",".join([f"{table_alias}.{field}" for field in fields])
    query = (f"SELECT {fields_str}"
             f" FROM {from_target} as {table_alias}"
             f" LIMIT {limit} OFFSET {offset}")
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
      print(f"Drill validation error: {traceback.format_exc()}")
      return ValidationResult(False, [f"Invalid location: {path}"])
    return ValidationResult(True, [])
