"""Local file source implementation."""

from typing import Any, Mapping, Sequence, Iterable, Literal
from pydantic import BaseModel

from utils import DrillMixin


class LocalFile(BaseModel):
  type: Literal['local_file'] = 'local_file'
  location: str


class Source(DrillMixin):
  """Implements SourceProto protocol for Drill Local Files."""

  def get_data(self,
               source: Mapping[str, Any],
               connections: Sequence[Mapping[str, Any]],
               fields: Sequence[str]) -> Iterable[Any]:
    location = source['location']
    conn_name = 'dfs'
    from_target = f'{conn_name}.`data/{location}`'
    return self.get_drill_data(from_target, fields)

  def schema(self):
    return LocalFile.schema_json()
