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

import json
import tempfile
from typing import Any, Dict, List, Mapping, Optional, Sequence

from google.auth.exceptions import RefreshError
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from pydantic import Field
from utils import ProtocolSchema, ValidationResult


class Source:
  """Implements SourceProto protocol for BigQuery."""

  def __init__(self, config: Dict[str, Any]):
    try:
      creds = json.loads(config.get("credentials"))
      config["credentials"] = creds
    except (ValueError, TypeError):
      # json.loads fails if credentials are not a valid JSON object
      config["credentials"] = None
    if config.get("credentials"):
      with tempfile.NamedTemporaryFile(
          mode="w", encoding="utf-8", delete=False
      ) as credentials_file:
        json.dump(config.get("credentials"), credentials_file)
        credentials_path = credentials_file.name
      self.client = bigquery.Client.from_service_account_json(credentials_path)
    else:
      self.client = bigquery.Client()
    self.location = f"{config.get('dataset')}.{config.get('table')}"

  def get_data(
      self,
      fields: Sequence[str],
      offset: int,
      limit: int,
      reusable_credentials: Optional[Sequence[Mapping[str, Any]]],
  ) -> List[Mapping[str, Any]]:
    """get_data implemention for BigQuery source."""
    query = (
        f"SELECT *"
        f" FROM `{self.location}`"
        f" LIMIT {limit} OFFSET {offset}"
    )
    query_job = self.client.query(query)

    rows = []
    for element in query_job.result():
      # create dict to hold results and respect the return type
      row = {}
      for f in fields:
        if f in element.keys():
          row[f] = element.get(f)
      rows.append(row)

    return rows

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    return ProtocolSchema(
        "bigquery",
        [
            ("dataset", str, Field(
                description="The name of your BigQuery dataset.",
                validation="^[a-zA-Z0-9_]{1,1024}$")),
            ("table", str, Field(
                description="The name of your BigQuery table.",)),
            ("credentials", Optional[Mapping[str, str]], Field(
                default=None,
                description="The full credentials service-account JSON string. Not needed if your backend is located in the same GCP project as the BigQuery table.")),
        ]
    )

  def validate(self) -> ValidationResult:
    try:
      self.client.get_table(self.location)
      return ValidationResult(True, [])
    except RefreshError:
      return ValidationResult(
          False,
          ["Missing credentials file (required when running outside of GCP)."]
      )
    except NotFound:
      return ValidationResult(False, [f"Table {self.location} is not found."])

