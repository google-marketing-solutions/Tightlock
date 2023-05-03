"""Defines protocol for destination classes."""

from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Protocol,
    Sequence,
    runtime_checkable,
)

from utils import ValidationResult


@runtime_checkable
class DestinationProto(Protocol):

  def __init__(self, config: Dict[str, Any]):
    ...

  def send_data(self, input_data: List[Mapping[str, Any]]) -> None:
    ...

  def fields(self) -> Sequence[str]:
    ...

  def schema(self) -> Dict[str, Any]:
    ...

  def batch_size(self) -> int:
    ...

  def validate(self) -> ValidationResult:
    ...
