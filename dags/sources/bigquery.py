"""BigQuery source implementation."""

import json
import tempfile
from typing import Any, Iterable, Mapping, Sequence

from google.cloud import bigquery
from pydantic import BaseModel


class BigQueryConnection(BaseModel):
  credentials: Mapping[str, str]
  project_id: str
  dataset: str
  table: str


class Source:
  """Implements SourceProto protocol for BigQuery."""

  def get_data(
      self,
      source: Mapping[str, Any],
      connections: Mapping[str, Any],
      fields: Sequence[str],
      offset: int,
      limit: int,
  ) -> Iterable[Any]:
    """get_data implemention for BigQuery source."""
    bq_connection = BigQueryConnection.parse_obj(source)
    if bq_connection.credentials:
      with tempfile.NamedTemporaryFile(
          mode="w", encoding="utf-8", delete=False
      ) as credentials_file:
        json.dump(bq_connection.credentials, credentials_file)
        credentials_path = credentials_file.name
      client = bigquery.Client.from_service_account_json(credentials_path)
    else:
      client = bigquery.Client(project_id=bq_connection.project_id)
    location = f"""`{bq_connection.dataset}.{bq_connection.table}`"""
    fields_string = ",".join(fields)
    query = (
        f"SELECT {fields_string}"
        f" FROM {location}"
        f" ORDER BY {fields_string}"
        f" LIMIT {limit} OFFSET {offset}"
    )
    query_job = client.query(query)

    return query_job.result()

  def config_schema(self) -> str:
    return BigQueryConnection.schema_json()
