"""Registers activations dynamically from config."""

import datetime
import functools
import importlib.util
import pathlib

from airflow.decorators import dag
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
from destinations.destination_proto import DestinationProto
from sources.source_proto import SourceProto


class DAGBuilder:
  """Builder class for dynamic DAGs."""

  def __init__(self):
    self.latest_config = self._get_latest_config()

  def _import_entity(self,
                     source_name: str,
                     folder_name: str) -> SourceProto | DestinationProto:
    module_name = "".join(x.title() for x in source_name.split("_") if not x.isspace())
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

  def _build_dynamic_dag(self, activation, external_connections):
    """Dynamically creates a DAG based on a given activation."""
    dag_id = f"{activation['name']}_dag"
    
    # config objects defining source, activation and connection
    activation_source = activation["source"]
    activation_destination = activation["destination"]

    schedule = activation["schedule"]
    schedule_interval = schedule if schedule else None

    # actual implementations of each source and destination
    target_source = self._import_source(activation_source["type"]).Source()
    
    target_destination = self._import_destination(activation_destination["type"]).Destination()

    @dag(dag_id=dag_id, start_date=datetime.datetime.now(), schedule_interval=schedule_interval)
    def dynamic_generated_dag():
      def get_data(fields):
        source_connection = activation_source["external_connection"]
        connection = external_connections[source_connection] if source_connection else None
        location = activation_source["location"]
        return target_source.get_data(connection, location, fields)

      @task
      def process():
        fields = target_destination.fields()
        target_destination.send_data(get_data(fields))

      process()
    return dynamic_generated_dag

  def register_dags(self):
    external_connections = self.latest_config["external_connections"]

    for activation in self.latest_config["activations"]:
      dynamic_dag = self._build_dynamic_dag(activation, external_connections)

      # register dag by calling the dag object
      dynamic_dag()

builder = DAGBuilder()
builder.register_dags()
