import datetime
import importlib
from dataclasses import asdict
from typing import Any, Dict

from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from protocols.destination_proto import DestinationProto
from protocols.source_proto import SourceProto


def instance_from_name(
    module_name: str, class_name: str, config: Dict[str, Any]
) -> SourceProto | DestinationProto:
  module = importlib.import_module(module_name)
  class_ = getattr(module, class_name)
  return class_(config)


@dag(
    dag_id="validate_source",
    is_paused_upon_creation=False,
    start_date=datetime.datetime.now(),
    schedule_interval=None,
    render_template_as_native_obj=True,
)
def validate():
  def validate_source(
      source_name: str,
      source_config: Dict[str, Any],
  ):
    source = instance_from_name(f"sources.{source_name}", "Source", source_config)

    # TODO(b/279079875): Pass destination fields to source validate to check schema
    return asdict(source.validate())

  PythonOperator(
      task_id="validate_source",
      op_kwargs={
          "source_name": "{{ dag_run.conf.get('source_name') }}",
          "source_config": "{{ dag_run.conf.get('source_config')}}",
      },
      python_callable=validate_source,
  )


validate()
