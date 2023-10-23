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

from datetime import datetime
import enum
from dataclasses import asdict
import json
import traceback
from typing import Any

from airflow.api.common.delete_dag import delete_dag
from airflow.exceptions import DagNotFound
from airflow.decorators import dag
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from sources import retry
from utils.dag_utils import DagUtils

DAG_FETCH_SIZE = 500
MAX_TRIES = 3
SCHEDULE_DAG_ONCE = '@once'
REGISTER_RETRY_ERRORS = 'register_retry_errors'
DELETE_RETRY_ERRORS = 'delete_retry_errors'


class RetryColumnIndices(enum.Enum):
  """An enum mapping for retry columns names to indices."""
  CONNECTION_ID = 0
  UUID = 1
  DESTINATION_TYPE = 2
  DESTINATION_FOLDER = 3
  DESTINATION_CONFIG = 4
  NEXT_RUN = 5
  RETRY_NUM = 6
  DELETE = 7
  DATA = 8


class RetryDAGBuilder:
  """Builder class for dynamic DAGs."""

  def __init__(self):
    self.retries = self._get_retries()
    self.register_errors_var = REGISTER_RETRY_ERRORS
    Variable.set(key=self.register_errors_var,
                 value=[],
                 description='Report of dynamic retry DAG registration errors.')

    self.delete_errors_var = DELETE_RETRY_ERRORS
    Variable.set(key=self.delete_errors_var,
                 value=[],
                 description='Report of dynamic retry DAG deletion errors.')

  def _get_retries(self):
    sql_stmt = 'SELECT connection_id, uuid, destination_type, '\
               '       destination_folder, destination_config, next_run, '\
               '       retry_num, delete, data '\
               'FROM Retries WHERE next_run <= %s ORDER BY next_run'
    cursor = DagUtils.exec_postgres_command(sql_stmt, (datetime.now(),))
    return cursor

  def create_retry_dag(self, row):
    connection_id = row[RetryColumnIndices.CONNECTION_ID.value]
    retry_uuid = row[RetryColumnIndices.UUID.value]

    try:
      new_dag_name = f'{connection_id}_{retry_uuid}'

      source = retry.Source({
          'connection_id': connection_id,
          'retry_num': row[RetryColumnIndices.RETRY_NUM.value],
          'uuid': retry_uuid
      })

      dest_config = json.loads(row[RetryColumnIndices.DESTINATION_CONFIG.value])
      dest_entity = DagUtils.import_entity(
          row[RetryColumnIndices.DESTINATION_TYPE.value],
          row[RetryColumnIndices.DESTINATION_FOLDER.value])
      destination = dest_entity.Destination(dest_config)

      dynamic_dag = DagUtils.build_dynamic_dag(
          new_dag_name=new_dag_name,
          schedule=SCHEDULE_DAG_ONCE,
          target_source=source,
          target_destination=destination,
          dest_type=row[RetryColumnIndices.DESTINATION_TYPE.value],
          dest_folder=row[RetryColumnIndices.DESTINATION_FOLDER.value],
          dest_config=row[RetryColumnIndices.DESTINATION_CONFIG.value])
      dynamic_dag()  # register dag by calling the dag object
    except Exception:  # pylint: disable=broad-except
      DagUtils.handle_errors(error_var=self.register_errors_var,
                             connection_id=connection_id,
                             log_msg=f'{connection_id} registration error',
                             error_traceback=traceback.format_exc())

  def delete_dag_marked_for_deletion(self, connection_id):
    try:
      delete_dag(connection_id)
    except DagNotFound as e:
      print(f'{e}. Will remove from Retries table shortly.')
    except Exception as e:
      DagUtils.handle_errors(error_var=self.register_errors_var,
                             connection_id=connection_id,
                             log_msg=f'{connection_id} deletion error',
                             error_traceback=traceback.format_exc())

  def check_for_retries(self):
    """Creates DAGs for retries."""
    dags_to_remove = []

    while True:
      rows = self.retries.fetchmany(DAG_FETCH_SIZE)
      if not rows:
        break

      for row in rows:
        if row[RetryColumnIndices.DELETE.value]:
          self.delete_dag_marked_for_deletion(
              row[RetryColumnIndices.CONNECTION_ID.value])
          dags_to_remove.append(row[RetryColumnIndices.UUID.value])
        else:
          self.create_retry_dag(row)

    if dags_to_remove:
      removal_list = "'" + "', '".join(dags_to_remove) + "'"
      DagUtils.exec_postgres_command('DELETE FROM Retries WHERE uuid IN (%s)',
                                    (removal_list,), True)


builder = RetryDAGBuilder()
builder.check_for_retries()
