"""Defines protocol for source classes."""

from typing import Any, Dict, Protocol, Sequence, Iterable


class SourceProto(Protocol):
  def get_data(self,
               connection: Dict[str, Any],
               location: str,
               fields: Sequence[str]) -> Iterable[Any]:
    ...
