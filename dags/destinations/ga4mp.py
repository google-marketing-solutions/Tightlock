"""GA4 MP destination implementation."""

from typing import Any, Dict, Iterable


class Destination:
  """Implements DestinationProto protocol for GA4 Measurement Protocol."""

  # TODO(b/265715341): Implement GA4 MP activation function
  def send_data(self, config: Dict[str, Any], input_data: Iterable[Any]):
    rows = input_data.fetchall()
    print(f"Config: {config}")
    print(f"Rows: {rows}")
    return rows

  # TODO(b/265715582): Implement GA4 MP input data query
  def fields(self, config: Dict[str, Any]):
    print(f"Config: {config}")
    # TODO(stocco): Include either 'app_instance_id' or 'client_id' depending on
    # the type (app or web).
    return ["user_id", "event_name", "engagement_time_msec", "session_id"]
