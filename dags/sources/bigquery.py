"""BigQuery source implementation."""

from typing import Any, Mapping, Sequence, Iterable
from pydantic import BaseModel
from google.cloud import bigquery


class BigQueryConnection(BaseModel):
  credentials: str


class Source():
  """Implements SourceProto protocol for BigQuery."""


  def get_data(self,
               source: Mapping[str, Any],
               connections: Mapping[str, Any],
               fields: Sequence[str],
               offset: int,
               limit: int) -> Iterable[Any]:
    location = source["location"]
    external_connection = source["external_connection"]
    if external_connection and external_connection in connections:
      bq_connection = BigQueryConnection.parse_obj(connections[external_connection])
      client = bigquery.Client(credentials=bq_connection.credentials)
    else:
      client = bigquery.Client()
    fields_string = ",".join(fields)
    query = (
      f"SELECT {fields_string}"
      f"FROM {location}"
      f"ORDER BY {fields_string}"
      f"LIMIT {limit} OFFSET {offset}"
    )
    query_job = client.query(query)
    
    return query_job.result()
  
  
  def config_schema(self) -> str:
    return BigQueryConnection.schema_json()