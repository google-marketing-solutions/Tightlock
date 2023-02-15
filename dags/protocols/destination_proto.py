"""Defines protocol for destination classes."""

from typing import Any, Protocol, Sequence, Iterable, runtime_checkable


@runtime_checkable
class DestinationProto(Protocol):
  def send_data(self, input_data: Iterable[Any]) -> None:
    ...

  def fields(self) -> Sequence[str]:
    ...
