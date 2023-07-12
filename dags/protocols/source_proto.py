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

from utils import ProtocolSchema, ValidationResult


@runtime_checkable
class SourceProto(Protocol):
  """Common set of methods that must be implemented by all sources."""

  def __init__(self, config: Dict[str, Any]):
    """Init method for SourceProto.
    
    Args:
      config: A dictionnary parsed from the connection config, containing
      the metadata required for the the target source.
    """
    ...

  def get_data(
      self,
      fields: Sequence[str],
      offset: int,
      limit: int,
      reusable_credentials: Optional[Sequence[Mapping[str, Any]]],
  ) -> List[Mapping[str, Any]]:
    """Retrieves data from the target source.
    
    Args:
      fields: A list of fields to be retrieved from the 
        underlying source.
      offset: The offset for the query.
      limit: The maximum number of records to return.
      reusable_credentials: An auxiliary list of reusable credentials
        that may be shared by multiple sources.
    Returns:
      A list of field-value mappings retrieved from the target data source.
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

  def validate(self) -> ValidationResult:
    """Validates the provided config.
    
    Returns:
      A ValidationResult for the provided config.
    """
    ...
