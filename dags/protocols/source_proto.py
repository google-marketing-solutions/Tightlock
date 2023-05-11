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

"""Defines protocol for source classes."""
from typing import (Any, Dict, List, Mapping, Protocol, Sequence, runtime_checkable, Optional)

from utils import ValidationResult


@runtime_checkable
class SourceProto(Protocol):

  def __init__(self, config: Dict[str, Any]):
    ...

  def get_data(
      self,
      connections: Sequence[Mapping[str, Any]],
      fields: Sequence[str],
      offset: int,
      limit: int,
  ) -> List[Mapping[str, Any]]:
    ...

  def schema(self) -> Dict[str, Any]:
    ...

  def validate(self) -> ValidationResult:
    ...
