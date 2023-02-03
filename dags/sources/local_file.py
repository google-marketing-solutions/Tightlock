"""Local file source implementation."""

from typing import Any, Dict, Sequence, Iterable
from sources.mixins import DrillMixin
from sources.source_proto import SourceProto


class Source(SourceProto, DrillMixin):
  """Implements SourceProto protocol for Drill Local Files."""

  def get_data(self,
               connection: Dict[str, Any],
               location: str,
               fields: Sequence[str]) -> Iterable[Any]:
    conn_name = "dfs"
    from_target = f"{conn_name}.`data/{location}`"
    return self.get_drill_data(from_target, fields)
