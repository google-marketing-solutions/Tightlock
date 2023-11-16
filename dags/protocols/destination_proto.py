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

from typing import (Any, Dict, List, Mapping, Optional, Protocol, Sequence,
                    runtime_checkable)

from utils.protocol_schema import ProtocolSchema
from utils.run_result import RunResult
from utils.validation_result import  ValidationResult


@runtime_checkable
class DestinationProto(Protocol):
  """Common set of methods that must be implemented by all destinations."""

  def __init__(self, config: Dict[str, Any]):
    """Init method for DestinationProto.

    Args:
      config: A dictionary parsed from the connection config, containing
        the metadata required for the the target destination.
    """
    ...

  def send_data(
      self, input_data: List[Mapping[str, Any]], dry_run: bool
  ) -> Optional[RunResult]:
    """Sends data to the underlying destination API.

    Args:
      input_data: A list of key-value mappings representing inputs for the
        target destination. The mapping keys must be a subset of the list
        retrieved by calling the destinations "fields" method.
      dry_run: A boolean indicating whether or not to actually send
        the data to the destination or if it should only be validated
        (when validation is available).

    Returns:
      An optional RunResult object, that can only be
      None in the event of an unexpected error.
    """
    ...

  def fields(self) -> Sequence[str]:
    """Lists required fields for the destination input data.

    Returns:
      A sequence of fields.
    """
    ...

  def batch_size(self) -> int:
    """Returns the required batch_size for the underlying destination API.

    Returns:
      An int representing the batch_size.
    """
    ...

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    """Returns the required metadata for this destination config.

    Returns:
      An optional ProtocolSchema object that defines the
      required and optional metadata used by the implementation
      of this protocol.
    """
    ...

  def validate(self) -> ValidationResult:
    """Validates the provided config.

    Returns:
      A ValidationResult for the provided config.
    """
    ...
