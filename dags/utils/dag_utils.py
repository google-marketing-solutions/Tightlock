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
from contextlib import closing
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

from airflow.decorators import dag
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
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
      new_dag_name: str,
      schedule: str,
      target_source: SourceProto,
      target_destination: DestinationProto,
      dest_type: str,
      dest_folder: str,
      dest_config: Mapping[str, Any],
      reusable_credentials: Optional[Sequence[Any]] = None,
  ):
    """Dynamically creates a DAG based on a given connection."""
    connection_id = f'{new_dag_name}_dag'
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

        run_result = RunResult(0, 0, [], [], dry_run)
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
        print(f'=====================DAG "{connection_id}", uuid={source_uuid}')

        if run_result.retriable_events:
          cls.schedule_retries(parent_conn_id=connection_id,
                               source_uuid=source_uuid,
                               run_result=run_result,
                               target_source=target_source,
                               dest_type=dest_type,
                               dest_folder=dest_folder,
                               dest_config=dest_config,
                               retry_batch_size=target_destination.batch_size())

        task_instance.xcom_push("run_result", asdict(run_result))

        if source_uuid:
          cls.mark_dag_for_deletion(source_uuid)

      PythonOperator(
          task_id=connection_id,
          op_kwargs={"dry_run_str": "{{dag_run.conf.get('dry_run', False)}}"},
          python_callable=process,
      )

    return dynamic_generated_dag

  @classmethod
  def schedule_retries(
      cls,
      parent_conn_id: str,
      source_uuid: str,
      run_result: RunResult,
      target_source: SourceProto,
      dest_type: str,
      dest_folder: str,
      dest_config: str,
      retry_batch_size: int,
  ):
    """Schedules a retry for the retriable events.

    Schedules up to `MAX_TRIES` tries. Otherwise, logs the failure.

    Args:
      parent_conn_id (str): The connection id of the parent.
      uuid (str): UUID, if Retry, else `''`.
      run_result (RunResult): The run result.
      target_source (SourceProto): The target source.
      dest_type (str): The target destination type.
      dest_folder (str): The target destination folder.
      dest_config (str): The target destination type config.
      retry_batch_size (int): The number of retriable events per retry row.
    """
    if (not run_result.successful_hits):
      retry_num = getattr(target_source, 'retry_num', 0) + 1
      print(f'DAG "{parent_conn_id}" had no successes and '
            f'{len(run_result.retriable_events)} retriable failures.'
            f' Incrementing retry count to {retry_num}.')
    else:

      print(f'DAG "{parent_conn_id}" had some successes and '
            f' {len(run_result.retriable_events)} retriable failures.'
            f' Resetting retry count to 0.')
      retry_num = 0

    if retry_num < MAX_TRIES:
      print(f'Retrying DAG "{parent_conn_id}" ({retry_num+1} of {MAX_TRIES}))')

      for r in range(0, run_result, len(run_result.retriable_events),
                     retry_batch_size):
        cls._add_retry_row(new_connection_id=parent_conn_id.strip(source_uuid),
                           new_uuid=str(uuid.uuid4()),
                           dest_type=dest_type,
                           dest_folder=dest_folder,
                           dest_config=dest_config,
                           retry_num=retry_num,
                           retriable_events=json.dumps(
                               run_result.retriable_events[r:r +
                                                           retry_batch_size]))
    else:
      print(f'DAG run "{parent_conn_id}" had {run_result.failed_hits} failures.'
            f'Not rescheduling due to max tries ({MAX_TRIES}).')

  @classmethod
  def _add_retry_row(cls, new_connection_id, new_uuid, dest_type, dest_folder,
                     dest_config, retry_num, retriable_events):
    sql = 'INSERT INTO Retries(connection_id, uuid, destination_type, '\
          '                    destination_folder, destination_config, next_run, '\
          '                    retry_num, delete, data) '\
          'VALUES (%s, %s, %s, %s, %s, %s, %s, false, %s)'
    wait_seconds = 60 * (int(
        round(10**(retry_num - 1) + random.uniform(0, 10**(retry_num - 2)), 0)))
    next_run = datetime.datetime.now() + datetime.timedelta(
        seconds=wait_seconds)

    closing(
        cls.exec_postgres_command(
            sql,
            (new_connection_id, new_uuid, dest_type, dest_folder,
             json.dumps(dest_config), next_run, retry_num, retriable_events),
            True))

  @classmethod
  def mark_dag_for_deletion(cls, dag_uuid):
    sql = 'UPDATE Retries SET delete = true WHERE uuid=%s'
    closing(cls.exec_postgres_command(sql, (dag_uuid,), True))

  @classmethod
  def exec_postgres_command(
      cls,
      command: str,
      parameters: Sequence = (),
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

  @classmethod
  def handle_errors(cls, error_var, connection_id, log_msg, error_traceback):
    register_errors = Variable.get(error_var, deserialize_json=True)
    register_errors.append({
        "connection_id": connection_id,
        "error": error_traceback
    })
    print(f"{log_msg} : {error_traceback}")

    Variable.update(error_var, register_errors, serialize_json=True)
