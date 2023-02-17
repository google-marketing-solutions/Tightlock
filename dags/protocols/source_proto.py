"""Defines protocol for source classes."""

from typing import Any, Mapping, Protocol, Iterable, Sequence, Dict, runtime_checkable


@runtime_checkable
class SourceProto(Protocol):
  def get_data(self,
               source: Mapping[str, Any],
               connections: Mapping[str, Any],
               fields: Sequence[str],
               offset: int,
               limit: int) -> Iterable[Any]:
    ...

  def schema(self) -> Dict[str, Any]:
    ...
