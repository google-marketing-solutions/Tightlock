"""Registers activations dynamically from config."""

import datetime
import functools
import importlib.util
import pathlib
from typing import Any, Mapping, Sequence

from airflow.decorators import dag
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
from protocols.destination_proto import DestinationProto
from protocols.source_proto import SourceProto


class DAGBuilder:
  """Builder class for dynamic DAGs."""

  def __init__(self):
    self.latest_config = self._get_latest_config()

  def _import_entity(self,
                     source_name: str,
                     folder_name: str) -> SourceProto | DestinationProto:
    module_name = "".join(
        x.title() for x in source_name.split("_") if not x.isspace())

    lower_source_name = source_name.lower()
    filepath = pathlib.Path(f"dags/{folder_name}") / f"{lower_source_name}.py"
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

  _import_source = functools.partialmethod(_import_entity,
                                           folder_name="sources")
  _import_destination = functools.partialmethod(_import_entity,
                                                folder_name="destinations")

  def _get_latest_config(self):
    sql_stmt = "SELECT value FROM Config ORDER BY create_date DESC LIMIT 1"
    pg_hook = PostgresHook(
        postgres_conn_id="tightlock_config",
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchone()[0]

  def _load_activation_modules(self, activation):
    pass

  def _build_dynamic_dag(self,
                         activation: Mapping[str, Any],
                         external_connections: Sequence[Any],
                         target_source: SourceProto,
                         target_destination: DestinationProto):
    """Dynamically creates a DAG based on a given activation."""
    dag_id = f"{activation['name']}_dag"

    schedule = activation["schedule"]
    schedule_interval = schedule if schedule else None

    @dag(dag_id=dag_id,
         is_paused_upon_creation=False,
         start_date=datetime.datetime.now(),
         schedule_interval=schedule_interval)
    def dynamic_generated_dag():
      @task
      def process():
        fields = target_destination.fields(activation["destination"])
        data = target_source.get_data(activation["source"],
                                      external_connections, fields)
        target_destination.send_data(activation["destination"], data)

      process()
    return dynamic_generated_dag

  def register_dags(self):
    """Loops over all configured activations and create an Airflow DAG for each one of them."""
    external_connections = self.latest_config["external_connections"]

    for activation in self.latest_config["activations"]:
    # actual implementations of each source and destination
      target_source = self._import_source(activation["source"]["type"]).Source()
      target_destination = self._import_destination(
          activation["destination"]["type"]).Destination()

      dynamic_dag = self._build_dynamic_dag(activation,
                                            external_connections,
                                            target_source,
                                            target_destination)

      # register dag by calling the dag object
      dynamic_dag()

builder = DAGBuilder()
builder.register_dags()
