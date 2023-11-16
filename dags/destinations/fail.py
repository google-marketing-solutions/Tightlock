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

"""This destination is supposed to fail and be gracefully handled."""


from typing import Any, Dict, List, Mapping, Optional, Sequence

from utils.protocol_schema import ProtocolSchema
from utils.run_result import RunResult
from utils.validation_result import ValidationResult


class Destination:
  """Implements DestinationProto protocol."""

  def __init__(self, config: Dict[str, Any]):
    raise Exception("Thuo shan't instantiate me!!!")

  def send_data(
      self, input_data: List[Mapping[str, Any]], dry_run: bool
  ) -> Optional[RunResult]:
    print(f"input_data: {input_data} and dry_run: {dry_run}")

  def fields(self) -> Sequence[str]:
    return ["foo", "bar", "qux", "zap"]

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    return None

  def batch_size(self) -> int:
    return 42

  def validate(self) -> ValidationResult:
    return ValidationResult(False, ["I'm always invalid."])
