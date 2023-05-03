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
