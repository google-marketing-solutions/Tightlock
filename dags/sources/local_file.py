"""Local file source implementation."""

from typing import Any, Dict, List, Literal, Mapping, Sequence

from pydantic import BaseModel
from dags.utils import DrillMixin
from dags.utils import ValidationResult


class LocalFile(BaseModel):
  type: Literal['local_file'] = 'local_file'
  location: str


class Source(DrillMixin):
  """Implements SourceProto protocol for Drill Local Files."""

  def __init__(self, config: Dict[str, Any]):
    self.config = config
    self.location = self.config['location']
    self.conn_name = 'dfs'
    self.path = f'{self.conn_name}.`data/{self.location}`'

  def get_data(
      self,
      connections: Sequence[Mapping[str, Any]],
      fields: Sequence[str],
      offset: int,
      limit: int,
  ) -> List[Mapping[str, Any]]:
    return self.get_drill_data(self.path, fields, offset, limit)

  def schema(self) -> Dict[str, Any]:
    return LocalFile.schema_json()

  def validate(self) -> ValidationResult:
    return self.validate_drill(self.path)
