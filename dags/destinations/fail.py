"""This destination is supposed to fail and be gracefully handled."""


from typing import Any, Dict, List, Mapping, Sequence

from utils import ValidationResult


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

  def validate(self) -> ValidationResult:
    return ValidationResult(False, ["I'm always invalid."])
