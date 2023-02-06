"""Defines protocol for destination classes."""

from typing import Any, Protocol, Sequence, Iterable


class DestinationProto(Protocol):
  def send_data(self, input_data: Iterable[Any]) -> None:
    ...

  def fields(self) -> Sequence[str]:
    ...
