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

from typing import Any, Dict, List, Mapping, Optional, Sequence

from pydantic import Field
from utils.drill_mixin import DrillMixin
from utils.protocol_schema import ProtocolSchema
from utils.validation_result import  ValidationResult


class Source(DrillMixin):
  """Implements SourceProto protocol for Drill Local Files."""

  def __init__(self, config: Dict[str, Any]):
    self.config = config
    self.location = self.config["location"]
    self.conn_name = "dfs"
    self.path = f"{self.conn_name}.`data/{self.location}`"

  def get_data(
      self,
      fields: Sequence[str],
      offset: int,
      limit: int,
      reusable_credentials: Optional[Sequence[Mapping[str, Any]]],
  ) -> List[Mapping[str, Any]]:
    return self.get_drill_data(self.path, fields, offset, limit)

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    return ProtocolSchema(
        "local_file",
        [
            ("location", str, Field(
                description="The path to your local file, relative to the container 'data' folder."))
        ]
    )

  def validate(self) -> ValidationResult:
    return self.validate_drill(self.path)
