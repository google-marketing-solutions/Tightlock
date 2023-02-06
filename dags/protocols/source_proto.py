"""Defines protocol for source classes."""

from typing import Any, Mapping, Protocol, Iterable, Sequence


class SourceProto(Protocol):
  def get_data(self,
               source: Mapping[str, Any],
               connections: Sequence[Mapping[str, Any]],
               fields: Sequence[str]) -> Iterable[Any]:
    ...
