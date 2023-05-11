"""
 Copyright 2023 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 """

# TODO(b/270748315): Implement actual schemas DAG using this as example

from typing import Literal, Union, Annotated, Dict, Optional, Sequence

from pydantic import BaseModel
from pydantic import Field


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


GA4MP = Annotated[Union[GA4Web, GA4App], Field(discriminator='event_type')]


class LocalFile(BaseModel):
  type: Literal['local_file'] = 'local_file'
  location: str

class BigQuery(BaseModel):
  project: str
  dataset: str
  table: str
  credentials: Optional[str]

class CustomerMatch(BaseModel):
  developer_token: str
  client_id: str
  client_secret: str
  audience_name: str
  hashed_in_place: Optional[bool] = False
  ingestion_type: Literal['Add', 'Remove', 'Replace']

Sources = Sequence[Union[LocalFile, BigQuery]]
Destinations = Sequence[Union[GA4MP, CustomerMatch]]

class Schemas(BaseModel):
  sources: Sources
  destinations: Destinations

print(Schemas.schema_json(indent=2))
