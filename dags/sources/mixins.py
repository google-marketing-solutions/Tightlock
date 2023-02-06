"""A Drill mixin that provides a get_drill_data wrapper for other classes that use drill"""

from typing import Any, Iterable, Sequence

from airflow.providers.apache.drill.hooks.drill import DrillHook


class DrillMixin:
  def get_drill_data(self, from_target: Sequence[str], fields: str) -> Iterable[Any]:
    drill_conn = DrillHook().get_conn()
    cursor = drill_conn.cursor()
    fields_str = ",".join(fields)
    query = f"SELECT {fields_str} FROM {from_target}"
    cursor.execute(query)
    return cursor
