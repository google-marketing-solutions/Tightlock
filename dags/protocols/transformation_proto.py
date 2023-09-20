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
from enum import Enum
from utils import ProtocolSchema, ValidationResult

@runtime_checkable
class TransformationProto(Protocol):
  """Common set of methods that must be implemented by all sources."""

  class Type(Enum):
    PRE = 0
    POST = 1

  def __init__(self, config: Dict[str, Any]):
    """Init method for SourceProto.

    Args:
      config: A dictionnary parsed from the connection config, containing
      the metadata required for the the target source.
    """
    ...

  def type(self) -> Type:
    """Returns the type of transformation.

    Returns:
      The type of transformation.
    """
    ...

  def pre_transform(
      self,
      fields: List[str]
  ) -> List[str]:
    """Runs pre source data pull to transform the fields.

    Args:
      fields: A list of fields to transform (add/edit/update/etc).

    Returns:
      A list of fields, transformed from the input fields.
    """
    ...

  def post_transform(
      self,
      input_data: List[Mapping[str, Any]]
  ) -> List[Mapping[str, Any]]:
    """Runs post source data pull to transform the values.

    Args:
      input_data: A list of field-value mappings to be transformed.

    Returns:
      A list of field-value mappings, transformed from the input data.
    """
    ...

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    """Returns the required metadata for this source config.

    Returns:
      An optional ProtocolSchema object that defines the
      required and optional metadata used by the implementation
      of this protocol.
    """
    ...

# Might or might not need.
#   def validate(self) -> ValidationResult:
#     """Validates the provided config.

#     Returns:
#       A ValidationResult for the provided config.
#     """
#     ...
