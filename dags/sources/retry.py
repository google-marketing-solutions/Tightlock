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

from contextlib import closing
from airflow.hooks.postgres_hook import PostgresHook
from typing import Any, Dict, List, Mapping, Optional, Sequence

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from pydantic import Field
from protocols.source_proto import SourceProto
from utils.dag_utils import DagUtils
from utils.protocol_schema import ProtocolSchema
from utils.validation_result import ValidationResult


class Source(SourceProto):
  """Implements SourceProto protocol for Retrying (should not be used directly)."""

  def __init__(self, config: Mapping[str, Any]):
    self.connection_id = config['connection_id']
    self.retry_num = config['retry_num']
    self.uuid = config['uuid']

    self.data = self._get_retry_data(config['connection_id'], config['uuid'])

  def _get_retry_data(self, conn_id, uuid) -> List[Mapping[str, Any]]:
    # NOTE: This source is only callable from `dags/retries.py`. It won't be
    #       called every 10 seconds from `dags/register_connections.py`.
    """Gets retry data from the database."""
    sql = f'''SELECT data
              FROM Retries
              WHERE connection_id = %s AND uuid = %s
              ORDER BY id ASC
              LIMIT 1'''
    with closing(DagUtils.exec_postgres_command(sql, (conn_id, uuid))) as c:
      return c.fetchone()[0]

  def get_data(
      self,
      fields: Sequence[str],
      offset: int,
      limit: int,
      reusable_credentials: Optional[Sequence[Mapping[str, Any]]],
  ) -> List[Mapping[str, Any]]:
    """`get_data()` implemention for Retry source."""
    return self.data[offset:offset + limit]

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    return None # Hides from UI.

  def validate(self) -> ValidationResult:
    return ValidationResult(True, [])
