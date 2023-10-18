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
limitations under the License."""
"""Registers connections dynamically from config."""

import datetime
import enum
from dataclasses import asdict
import traceback
from typing import Any

from airflow.api.common.delete_dag import delete_dag
from airflow.decorators import dag
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator

from sources import retry
from utils.dag_utils import DagUtils

MAX_TRIES = 3


class RetryColumnIndices(enum.Enum):
  """An enum mapping for retry columns names to indices."""
  CONNECTION_ID = 0
  UUID = 1
  DESTINATION_TYPE = 2
  DESTINATION_FOLDER = 3
  DESTINATION_CONFIG = 4
  NEXT_RUN = 5
  RETRY_NUM = 6
  DATA = 7


class RetryDAGBuilder:
  """Builder class for dynamic DAGs."""

  def __init__(self):
    self.retries = self._get_retries()
    self.register_errors_var = "register_retry_errors"
    Variable.set(key=self.register_errors_var,
                 value=[],
                 description="Report of dynamic retry DAG registering errors.")

  def _get_retries(self):
    sql_stmt = 'SELECT connection_id, uuid, destination_type, '\
               '       destination_folder, destination_config, next_run, '\
               '       retry_num, data '\
               'FROM Retries WHERE next_run <= %s ORDER BY next_run'
    pg_hook = PostgresHook(postgres_conn_id="tightlock_config",)
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt, (datetime.datetime.now(),))
    return cursor

  def check_for_retries(self):
    """Creates DAGs for retries."""
    while True:
      rows = self.retries.fetchmany(500)
      if not rows:
        break

      for row in rows:
        # actual implementations of each source and destination
        try:
          connection_id = row[RetryColumnIndices.CONNECTION_ID.value]
          retry_uuid = row[RetryColumnIndices.UUID.value]
          connection_name = f'{connection_id}_{retry_uuid}'
          source = retry.Source({
              'connection_id': connection_id,
              'retry_num': row[RetryColumnIndices.RETRY_NUM.value],
              'uuid': retry_uuid
          })
          destination = DagUtils.import_entity(
              row[RetryColumnIndices.DESTINATION_TYPE.value],
              row[RetryColumnIndices.DESTINATION_FOLDER.value]).Destination(
                  row[RetryColumnIndices.DESTINATION_CONFIG.value])

          dynamic_dag = DagUtils.build_dynamic_dag(
              connection_name=connection_name,
              schedule=None,
              target_source=source,
              target_destination=destination,
              dest_type=row[RetryColumnIndices.DESTINATION_TYPE.value],
              dest_folder=row[RetryColumnIndices.DESTINATION_FOLDER.value],
              dest_config=row[RetryColumnIndices.DESTINATION_CONFIG.value])
          dynamic_dag()  # register dag by calling the dag object
        except Exception:  # pylint: disable=broad-except
          error_traceback = traceback.format_exc()
          register_errors = Variable.get(self.register_errors_var,
                                         deserialize_json=True)
          register_errors.append({
              "connection_id": connection_id,
              "error": error_traceback
          })
          print(f"{connection_id} registration error : {error_traceback}")

          Variable.update(self.register_errors_var,
                          register_errors,
                          serialize_json=True)


builder = RetryDAGBuilder()
builder.check_for_retries()
