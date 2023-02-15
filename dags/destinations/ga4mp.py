"""GA4 MP destination implementation."""

from typing import Any, Iterable


class Destination:
  """Implements DestinationProto protocol for GA4 Measurement Protocol."""

  # TODO(b/265715341): Implement GA4 MP activation function
  def send_data(self, input_data: Iterable[Any]):
    rows = input_data.fetchall()
    print(f"Rows: {rows}")
    return rows

  # TODO(b/265715582): Implement GA4 MP input data query
  def fields(self):
    return ["client_id", "event_name", "engagement_time_msec", "session_id"]
