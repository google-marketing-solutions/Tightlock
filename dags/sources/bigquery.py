"""BigQuery source implementation."""

import json
import tempfile
from typing import Any, Dict, List, Optional, Mapping, Sequence

from google.cloud import bigquery
from pydantic import BaseModel


class BigQueryConnection(BaseModel):
  credentials: Optional[Mapping[str, str]] = None
  dataset: str
  table: str


class Source:
  """Implements SourceProto protocol for BigQuery."""

  def __init__(self, config: Dict[str, Any]):
    self.config = config

  def get_data(
      self,
      connections: Sequence[Mapping[str, Any]],
      fields: Sequence[str],
      offset: int,
      limit: int,
  ) -> List[Mapping[str, Any]]:
    """get_data implemention for BigQuery source."""
    bq_connection = BigQueryConnection.parse_obj(self.config)
    if bq_connection.credentials:
      with tempfile.NamedTemporaryFile(
          mode="w", encoding="utf-8", delete=False
      ) as credentials_file:
        json.dump(bq_connection.credentials, credentials_file)
        credentials_path = credentials_file.name
      client = bigquery.Client.from_service_account_json(credentials_path)
    else:
      client = bigquery.Client()
    location = f"""`{bq_connection.dataset}.{bq_connection.table}`"""
    fields_string = ",".join(fields)
    query = (
        f"SELECT {fields_string}"
        f" FROM {location}"
        f" ORDER BY {fields_string}"
        f" LIMIT {limit} OFFSET {offset}"
    )
    query_job = client.query(query)

    rows = []
    for element in query_job.result():
      # create dict to hold results and respect the return type
      row = {}
      for field in fields:
        row[field] = element[field]
      rows.append(row)

    return rows

  def schema(self) -> Dict[str, Any]:
    return BigQueryConnection.schema_json()
