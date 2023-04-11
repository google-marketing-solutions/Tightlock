"""Defines protocol for source classes."""

from typing import Any, Mapping, Protocol, List, Sequence, Dict, runtime_checkable


@runtime_checkable
class SourceProto(Protocol):
  def get_data(self,
               source: Mapping[str, Any],
               connections: Sequence[Mapping[str, Any]],
               fields: Sequence[str],
               offset: int,
               limit: int) -> List[Mapping[str, Any]]:
    ...

  def schema(self) -> Dict[str, Any]:
    ...
