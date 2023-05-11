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
 limitations under the License.
 """

"""Local file source implementation."""
from typing import Any, Dict, List, Literal, Mapping, Sequence

from pydantic import BaseModel
from utils import DrillMixin
from utils import ValidationResult


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
