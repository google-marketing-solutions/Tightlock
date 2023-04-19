"""Defines protocol for source classes."""

from typing import Any, Mapping, Protocol, List, Sequence, Dict, runtime_checkable


@runtime_checkable
class SourceProto(Protocol):
  def __init__(self, config: Dict[str, Any]):
    ...

  def get_data(self,
               connections: Sequence[Mapping[str, Any]],
               fields: Sequence[str],
               offset: int,
               limit: int) -> List[Mapping[str, Any]]:
    ...

  def schema(self) -> Dict[str, Any]:
    ...
