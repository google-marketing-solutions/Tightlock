"""GA4 MP destination implementation."""

from typing import Any, Dict, Iterable, Optional, Literal, Annotated, Union

from pydantic import BaseModel
from pydantic import Field
from pydantic import schema_json


class GA4Base(BaseModel):
  type: Literal['GA4MP'] = 'GA4MP'
  api_secret: str
  non_personalized_ads: Optional[bool] = False
  debug: Optional[bool] = False
  user_properties: Optional[Dict[str, Dict[str, str]]]


class GA4Web(GA4Base):
  event_type: Literal['gtag']
  measurement_id: str


class GA4App(GA4Base):
  event_type: Literal['firebase']
  firebase_app_id: str


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

  def schema(self):
    GA4MP = Annotated[Union[GA4Web, GA4App], Field(discriminator='event_type')]

    return schema_json(GA4MP, title='GA4MP Destination Type', indent=2)
