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
"""Utility functions for DAGs."""

import ast
from dataclasses import asdict
import datetime
from functools import partial
import importlib
import json
import os
import pathlib
import random
import sys
from typing import Any, List, Mapping, Optional, Sequence
import uuid

from airflow.api.common.delete_dag import delete_dag
from airflow.decorators import dag
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
import psycopg2

from protocols.destination_proto import DestinationProto
from protocols.source_proto import SourceProto
from utils.run_result import RunResult

MAX_TRIES = 3


class DagUtils:
  """A set of utility functions for DAGs."""

  @classmethod
  def build_dynamic_dag(
      cls,
      connection_name: str,
      schedule: str,
      target_source: SourceProto,
      target_destination: DestinationProto,
      dest_type: str,
      dest_folder: str,
      dest_config: Mapping[str, Any],
      reusable_credentials: Optional[Sequence[Any]] = None,
  ):
    """Dynamically creates a DAG based on a given connection."""
    connection_id = f'{connection_name}_dag'
    schedule_interval = schedule if schedule else None
    start_date = datetime.datetime(2023, 1, 1, 0, 0, 0)

    @dag(dag_id=connection_id,
         is_paused_upon_creation=False,
         start_date=start_date,
         schedule_interval=schedule_interval,
         catchup=False)
    def dynamic_generated_dag():

      def process(task_instance, dry_run_str: str) -> None:
        dry_run = cls.parse_dry_run(connection_id, dry_run_str)
        fields = target_destination.fields()
        batch_size = target_destination.batch_size()
        offset = 0
        get_data = partial(target_source.get_data,
                           fields=fields,
                           limit=batch_size,
                           reusable_credentials=reusable_credentials)
        data = get_data(offset=offset)

        run_result = RunResult(0, 0, [], dry_run)
        while data:
          run_result += target_destination.send_data(data, dry_run)
          offset += batch_size
          ## TESTING ONLY ######################################################
          # TODO(blevitan): REMOVE this line before merging BLEVITAN_DEV branch into MAIN.
          run_result.retriable_events = data
          ######################################################################
          data = get_data(offset=offset)

        # only retries have uuid.
        source_uuid = getattr(target_source, 'uuid', '')

        if run_result.retriable_events:
          cls.schedule_retry(connection_id=connection_id,
                             source_uuid=source_uuid,
                             run_result=run_result,
                             target_source=target_source,
                             dest_type=dest_type,
                             dest_folder=dest_folder,
                             dest_config=dest_config)

        # TODO(blevitan): Replace with this without causing circular import.
        # if target_source.__module__ == Retry.__module__:
        if source_uuid:
          cls.delete_previous_retry_dag(connection_id, source_uuid)

        task_instance.xcom_push("run_result", asdict(run_result))

      print(
          '==========================================================CAN YOU SEE ME?'
      )
      PythonOperator(
          task_id=connection_id,
          op_kwargs={"dry_run_str": "{{dag_run.conf.get('dry_run', False)}}"},
          python_callable=process,
      )

    return dynamic_generated_dag

  @classmethod
  def schedule_retry(
      cls,
      connection_id: str,
      source_uuid: str,
      run_result: RunResult,
      target_source: SourceProto,
      dest_type: str,
      dest_folder: str,
      dest_config: str,
  ):
    """Schedules a retry for the retriable events.

    Schedules up to `MAX_TRIES` tries. Otherwise, logs the failure.

    Args:
      connection_id (str): The connection id.
      uuid (str): UUid, if Retry, else `''`.
      run_result (RunResult): The run result.
      target_source (SourceProto): The target source.
      dest_type (str): The target destination type.
      dest_folder (str): The target destination folder.
      dest_config (str): The target destination type config.
    """
    print('=====================_schedule_retry')
    if (not run_result.successful_hits):
      retry_num = getattr(target_source, 'retry_num', 0) + 1
      print(f'DAG "{connection_id}" had no successes and '
            f'{len(run_result.retriable_events)} retriable failures.'
            f' Incrementing retry count to {retry_num}.')
    else:
      print(f'DAG "{connection_id}" had some successes and '
            f' {len(run_result.retriable_events)} retriable failures.'
            f' Resetting retry count to 0.')
      retry_num = 0

    if retry_num < MAX_TRIES:
      print(f'Retrying DAG "{connection_id}" ({retry_num+1} of {MAX_TRIES}))')
      cls._add_retry_row(new_connection_id=connection_id.strip(source_uuid),
                         new_uuid=str(uuid.uuid4()),
                         dest_type=dest_type,
                         dest_folder=dest_folder,
                         dest_config=dest_config,
                         retry_num=retry_num,
                         retriable_events=json.dumps(
                             run_result.retriable_events))
    else:
      print(f'DAG run "{connection_id}" had {run_result.failed_hits} failures.'
            f'Not rescheduling due to max tries ({MAX_TRIES}).')

  @classmethod
  def _add_retry_row(cls, new_connection_id, new_uuid, dest_type,
                     dest_folder, dest_config, retry_num, retriable_events):
    sql = 'INSERT INTO Retries(connection_id, uuid, destination_type, '\
          '                    destination_folder, destination_config, next_run, '\
          '                    retry_num, data) '\
          'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
    wait_seconds = 60 * (int(
        round(10**(retry_num - 1) + random.uniform(0, 10**(retry_num - 2)), 0)))
    next_run = datetime.datetime.now() + datetime.timedelta(
        seconds=wait_seconds)

    cls.exec_postgres_command(
        sql, (new_connection_id, new_uuid, dest_type, dest_folder,
              dest_config, next_run, retry_num, retriable_events), True)

  @classmethod
  def delete_previous_retry_dag(cls, connection_id, prev_uuid):
    delete_dag(connection_id)
    sql = f'DELETE FROM Retry WHERE connection_id = %s AND uuid = %s'
    cls.exec_postgres_command(sql, (connection_id, prev_uuid), True)

  @classmethod
  def exec_postgres_command(
      cls,
      command: str,
      parameters: Sequence,
      autocommit: bool = False) -> psycopg2.extensions.cursor:
    """Execute a postgres command.

    Args:
        command (str): Command to execute.
        parameters (Sequence): Parameters for the command.
        autocommit (bool, optional): Whether to autocommit the command.
                                     Defaults to False.

    Raises:
        e: Exception raised by the command.

    Returns:
        psycopg2.extensions.cursor: Cursor for the command.
    """
    pg_hook: PostgresHook = PostgresHook(postgres_conn_id='tightlock_config',)
    conn: psycopg2.extensions.connection = pg_hook.get_conn()
    cursor: psycopg2.extensions.cursor = conn.cursor()
    try:
      cursor.execute(command, parameters)
      if autocommit:
        conn.commit()
    except Exception as e:
      if autocommit:
        conn.rollback()
      print(e)
      raise e
    return cursor

  @classmethod
  def import_modules_from_folder(cls, folder_name: str):
    """Import all modules from a given folder."""
    modules = []
    dags_path = f"airflow/dags/{folder_name}"
    folder_path = pathlib.Path().resolve().parent / dags_path
    for filename in os.listdir(folder_path):
      if os.path.isfile(folder_path / filename) and filename != "__init__.py":
        module_name, _ = filename.split(".py")
        module_path = os.path.join(folder_path, filename)
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        modules.append(module)
    return modules

  @classmethod
  def parse_dry_run(cls, connection_id: str, dry_run_str: str) -> bool:
    try:
      dry_run = ast.literal_eval(dry_run_str)
      if dry_run:
        print(f"Dry-run enabled for {connection_id}")
      return dry_run
    except ValueError:
      print(f"Dry-run defaulting to False for {connection_id}")
      return False

  @classmethod
  def import_entity(cls, source_name: str,
                    folder_name: str) -> SourceProto | DestinationProto:
    module_name = "".join(
        x.title() for x in source_name.split("_") if not x.isspace())

    lower_source_name = source_name.lower()
    filepath = pathlib.Path(f"dags/{folder_name}") / f"{lower_source_name}.py"
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
