"""Local file source implementation."""

from typing import Any, Mapping, Sequence, Iterable
from sources.mixins import DrillMixin


class Source(DrillMixin):
  """Implements SourceProto protocol for Drill Local Files."""

  def get_data(self,
               source: Mapping[str, Any],
               connections: Sequence[Mapping[str, Any]],
               fields: Sequence[str]) -> Iterable[Any]:
    location = source["location"]
    conn_name = "dfs"
    from_target = f"{conn_name}.`data/{location}`"
    return self.get_drill_data(from_target, fields)
