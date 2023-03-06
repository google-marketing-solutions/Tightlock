"""Defines protocol for destination classes."""

from typing import Any, Dict, Protocol, Sequence, Iterable, runtime_checkable


@runtime_checkable
class DestinationProto(Protocol):
  def __init__(self, config: Dict[str, Any]):
    ...

  def send_data(
      self, input_data: Iterable[Any]) -> None:
    ...

  def fields(self) -> Sequence[str]:
    ...

  def schema(self) -> Dict[str, Any]:
    ...
