"""GA4 MP destination implementation."""

import enum

from typing import Any, Dict, Iterable

_GA_EVENT_POST_URL = "https://www.google-analytics.com/mp/collect"
_GA_EVENT_VALIDATION_URL = "https://www.google-analytics.com/debug/mp/collect"

_FIREBASE_ID_COLUMN = "app_instance_id"
_GTAG_ID_COLUMN = "client_id"


class PayloadTypes(enum.Enum):
  """GA4 Measurememt Protocol supported payload types."""

  FIREBASE = "firebase"
  GTAG = "gtag"


class Destination:
  """Implements DestinationProto protocol for GA4 Measurement Protocol."""

  def __init__(self, config: Dict[str, Any]):
    self.config = config  # Keeping a reference for convenience.
    self.payload_type = config.get("payload_type")
    self.api_secret = config.get("api_secret")
    self.firebase_app_id = config.get("firebase_app_id")
    self.measurement_id = config.get("measurement_id")
    self.non_personalized_ads = config.get("non_personalized_ads") or False
    self.debug = config.get("debug") or False

    self._validate_credentials()
    self.post_url = self._build_api_url(True)
    self.validate_url = self._build_api_url(False)

    print(f"payload_type: {self.payload_type}")
    print(f"api_secret: {self.api_secret}")
    print(f"firebase_app_id: {self.firebase_app_id}")
    print(f"measurement_id: {self.measurement_id}")
    print(f"non_personalized_ads: {self.non_personalized_ads}")

  def send_data(self, input_data: Iterable[Any]):
    """Builds payload ans sends data to GA4MP API."""

    rows = input_data.fetchall()
    print(f"Rows: {rows}")
    return rows

  def fields(self):
    if self.payload_type == PayloadTypes.FIREBASE.value:
      id_column_name = _FIREBASE_ID_COLUMN
    else:
      id_column_name = _GTAG_ID_COLUMN
    return [id_column_name] + [
        "user_id",
        "event_name",
        "engagement_time_msec",
        "session_id",
    ]

  def _validate_credentials(self) -> None:
    """Validate credentials.

    Raises:
      Exception: If credential combination does not meet criteria.
    """
    if not self.api_secret:
      raise Exception(f"Missing api secret >>>>>> {self.config}")

    valid_payload_types = (PayloadTypes.FIREBASE.value, PayloadTypes.GTAG.value)
    if self.payload_type not in valid_payload_types:
      raise Exception(
          f"Unsupport payload_type: {self.payload_type}. Supported "
          "payload_type is gtag or firebase."
      )

    if self.payload_type == PayloadTypes.FIREBASE.value and not self.firebase_app_id:
      raise Exception(
          "Wrong payload_type or missing firebase_app_id. Please make sure "
          "firebase_app_id is set when payload_type is firebase."
      )

    if self.payload_type == PayloadTypes.GTAG.value and not self.measurement_id:
      raise Exception(
          "Wrong payload_type or missing measurement_id. Please make sure "
          "measurement_id is set when payload_type is gtag."
      )

  def _build_api_url(self, is_post: bool) -> str:
    """Builds the url for sending the payload.

    Args:
      is_post: true for building post url, false for building validation url.
    Returns:
      url: Full url that can be used for sending the payload
    """
    if self.payload_type == PayloadTypes.GTAG.value:
      query_url = "api_secret={}&measurement_id={}".format(
          self.api_secret, self.measurement_id
      )
    else:
      query_url = "api_secret={}&firebase_app_id={}".format(
          self.api_secret, self.firebase_app_id
      )
    if is_post:
      built_url = f"{_GA_EVENT_POST_URL}?{query_url}"
    else:
      built_url = f"{_GA_EVENT_VALIDATION_URL}?{query_url}"
    return built_url
