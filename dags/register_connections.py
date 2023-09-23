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

import ast
import datetime
import importlib.util
import json
import pathlib
from random import random
import re
import traceback
from dataclasses import asdict
from functools import partial
from typing import Any, Mapping, Optional, Sequence
import uuid

from airflow.decorators import dag
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from dags import sources
from fastapi import Depends, HTTPException
from protocols.destination_proto import DestinationProto
from protocols.source_proto import SourceProto
from sqlalchemy.exc import IntegrityError
from sqlmodel.ext.asyncio.session import AsyncSession
from tightlock_api import db
from utils import RunResult

MAX_TRIES = 3


class DAGBuilder:
  """Builder class for dynamic DAGs."""

  def __init__(self):
    self.latest_config = self._get_latest_config()
    self.register_errors_var = "register_errors"
    Variable.set(key=self.register_errors_var,
                 value=[],
                 description="Report of dynamic DAG registering errors.")

  def _config_from_ref(
      self, ref: Mapping[str, str]) -> SourceProto | DestinationProto:
    refs_regex = r"^#\/(sources|destinations)\/(.*)"
    ref_str = ref["$ref"]
    match = re.search(refs_regex, ref_str)
    target_folder = match.group(1)
    target_name = match.group(2)
    target_config = self.latest_config[target_folder][target_name]
    target_type = target_config.get("type")
    if not target_type:
      raise ValueError("Missing config attribute `type`.")
    if target_folder == "sources":
      return self._import_entity(target_type,
                                 target_folder).Source(target_config)
    elif target_folder == "destinations":
      return self._import_entity(target_type,
                                 target_folder).Destination(target_config)
    raise ValueError(f"Not supported folder: {target_folder}")

  def _parse_dry_run(self, connection_id: str, dry_run_str: str) -> bool:
    try:
      dry_run = ast.literal_eval(dry_run_str)
      if dry_run:
        print(f"Dry-run enabled for {connection_id}")
      return dry_run
    except ValueError:
      print(f"Dry-run defaulting to False for {connection_id}")
      return False

  def _import_entity(self, source_name: str,
                     folder_name: str) -> SourceProto | DestinationProto:
    module_name = "".join(
        x.title() for x in source_name.split("_") if not x.isspace())

    lower_source_name = source_name.lower()
    filepath = pathlib.Path(f"dags/{folder_name}") / f"{lower_source_name}.py"
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

  def _get_latest_config(self):
    sql_stmt = "SELECT value FROM Config ORDER BY create_date DESC LIMIT 1"
    pg_hook = PostgresHook(postgres_conn_id="tightlock_config",)
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchone()[0]

  def _build_dynamic_dag(
      self,
      connection: Mapping[str, Any],
      target_source: SourceProto,
      target_destination: DestinationProto,
      reusable_credentials: Optional[Sequence[Any]] = None,
  ):
    """Dynamically creates a DAG based on a given connection."""
    connection_id = f"{connection['name']}_dag"
    schedule = connection["schedule"]
    schedule_interval = schedule if schedule else None

    start_date = datetime.datetime(2023, 1, 1, 0, 0, 0)

    @dag(dag_id=connection_id,
         is_paused_upon_creation=False,
         start_date=start_date,
         schedule_interval=schedule_interval,
         catchup=False)
    def dynamic_generated_dag():

      def process(task_instance, dry_run_str: str) -> None:
        dry_run = self._parse_dry_run(connection_id, dry_run_str)
        fields = target_destination.fields()
        batch_size = target_destination.batch_size()
        offset = 0
        get_data = partial(target_source.get_data,
                           fields=fields,
                           limit=batch_size,
                           reusable_credentials=reusable_credentials)

        run_result = RunResult(0, 0, [], dry_run)
        while data:
          run_result += target_destination.send_data(data, dry_run)
          offset += batch_size
          data = get_data(offset=offset)

        if run_result.retriable_events:
          self._schedule_retry(connection_id, run_result, target_source,
                               target_destination)

        task_instance.xcom_push("run_result", asdict(run_result))

      PythonOperator(
          task_id=connection_id,
          op_kwargs={"dry_run_str": "{{dag_run.conf.get('dry_run', False)}}"},
          python_callable=process,
      )

    return dynamic_generated_dag

  async def _schedule_retry(self, connection_id: str, run_result: RunResult,
                            target_source: SourceProto,
                            target_destination: DestinationProto):
    """Schedules a retry for the retriable events.

    Schedules up to `MAX_TRIES` tries. Otherwise, logs the failure.

    Args:
      connection_id: The connection id.
      run_result: The run result.
      target_source: The target source.
      target_destination: The target destination.
    """
    if (not run_result.successful_hits):
      retry_count = target_source.retry_count + 1
      print(f'DAG "{connection_id}" had {run_result.failed_hits} retriable '
            f'failures without any successful.')
    else:
      print(f'DAG "{connection_id}" had {run_result.failed_hits} retriable '
            f'failures with some successful. Resetting retry count.')
      retry_count = 0

    if retry_count < MAX_TRIES:
      print(f'Retrying DAG "{connection_id}" ({retry_count+1} of {MAX_TRIES}).')
      await self._add_retry_db_row(connection_id, target_source.uuid,
                                   run_result)
      self._add_retry_dag(connection_id, target_destination, retry_count)
    else:
      print(f'DAG run "{connection_id}" had {run_result.failed_hits} failures.'
            f'Not rescheduling due to max tries ({MAX_TRIES}).')

  async def _add_retry_db_row(self, connection_id, prev_uuid, run_result):
    unique_id = uuid.uuid4()
    retry_data = {
        'connection_id': connection_id,
        'uuid': unique_id,
        'data': json.dumps(run_result.retriable_events)
    }

    # TODO(caiotomazelli): Make sure this is done properly for async, if possible.
    session: AsyncSession = db.get_session()
    session.add(retry_data)
    await session.commit()

    # TODO(caiotomazelli): Implement below with `session.delete()`, if possible.
    sql = f'''DELETE FROM Retry WHERE connection_id = %s AND uuid = %s'''
    pg_hook = PostgresHook(postgres_conn_id="tightlock_retry",)
    cursor = pg_hook.get_conn().cursor()
    cursor.execute(sql, (connection_id, prev_uuid))

  def _add_retry_dag(self, connection_id, target_destination, retry_count):
    target_source = sources.retry.Source({
        'connection_id': connection_id,
        'retry_count': retry_count,
        'uuid': uuid.uuid4()
    })
    wait_minutes = 10**(retry_count - 1) + random.uniform(
        0, 10**(retry_count - 2))
    now = datetime.datetime.now()
    schedule = f'{now.minute + wait_minutes} {now.hour} {now.day} {now.month} {now.weekday()}'
    activation = {
        'name': f"{connection_id[:-3]}_retry_{retry_count}",
        'source': target_source,
        'destination': target_destination.ref(),
        'schedule': schedule
    }
    retry_dag = self._build_dynamic_dag(activation, target_source,
                                        target_destination)
    retry_dag()

    # TODO(caiotomazelli): Is this how you delete the dag?
    self.delete()


  def register_dags(self):
    """Loops over all configured connections and create an Airflow DAG for each one of them."""
    # TODO(b/290388517): Remove mentions to activation once UI is ready
    for connection in self.latest_config["activations"]:
      # actual implementations of each source and destination
      try:
        target_source = self._config_from_ref(connection["source"])
        target_destination = self._config_from_ref(connection["destination"])
        dynamic_dag = self._build_dynamic_dag(connection, target_source,
                                              target_destination)
        # register dag by calling the dag object
        dynamic_dag()
      except Exception:  # pylint: disable=broad-except
        error_traceback = traceback.format_exc()
        register_errors = Variable.get(self.register_errors_var,
                                       deserialize_json=True)
        register_errors.append({
            "connection_name": connection["name"],
            "error": error_traceback
        })
        print(f"{connection['name']} registration error : {error_traceback}")

        Variable.update(self.register_errors_var,
                        register_errors,
                        serialize_json=True)


builder = DAGBuilder()
builder.register_dags()
