"""Defines protocol for destination classes."""

from typing import Any, Dict, Protocol, Sequence, List, runtime_checkable, Mapping


@runtime_checkable
class DestinationProto(Protocol):
  def __init__(self, config: Dict[str, Any]):
    ...

  def send_data(
      self, input_data: List[Mapping[str, Any]]) -> None:
    ...

  def fields(self) -> Sequence[str]:
    ...

  def schema(self) -> Dict[str, Any]:
    ...

  def batch_size(self) -> int:
    ...
