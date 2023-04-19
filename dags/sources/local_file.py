"""Local file source implementation."""

from typing import Any, Dict, Mapping, Sequence, List, Literal
from pydantic import BaseModel

from utils import DrillMixin


class LocalFile(BaseModel):
  type: Literal['local_file'] = 'local_file'
  location: str


class Source(DrillMixin):
  """Implements SourceProto protocol for Drill Local Files."""
  def __init__(self, config: Dict[str, Any]):
    self.config = config

  def get_data(self,
               connections: Sequence[Mapping[str, Any]],
               fields: Sequence[str],
               offset: int,
               limit: int
               ) -> List[Mapping[str, Any]]:
    location = self.config['location']
    conn_name = 'dfs'
    from_target = f'{conn_name}.`data/{location}`'
    return self.get_drill_data(from_target, fields, offset, limit)

  def schema(self) -> Dict[str, Any]:
    return LocalFile.schema_json()
