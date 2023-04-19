"""This destination is supposed to fail and be gracefully handled."""


from typing import Any, Dict, Iterable, List, Mapping, Sequence


class Destination:
  """Implements DestinationProto protocol."""

  def __init__(self, config: Dict[str, Any]):
    raise Exception("Thuo shan't instantiate me!!!")

  def send_data(self, input_data: List[Mapping[str, Any]]) -> None:
    print(f"input_data: {input_data}")

  def fields(self) -> Sequence[str]:
    return ["foo", "bar", "qux", "zap"]

  def schema(self) -> Dict[str, Any]:
    return {"foo": "bar"}

  def batch_size(self) -> int:
    return 42
