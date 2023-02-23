"""Defines protocol for destination classes."""

from typing import Any, Dict, Protocol, Sequence, Iterable, runtime_checkable


@runtime_checkable
class DestinationProto(Protocol):
  def send_data(
      self, config: Dict[str, Any], input_data: Iterable[Any]) -> None:
    ...

  def fields(self, config: Dict[str, Any]) -> Sequence[str]:
    ...
