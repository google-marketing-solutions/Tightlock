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

import datetime
import importlib
from dataclasses import asdict
from typing import Any, Dict, Optional

from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from protocols.destination_proto import DestinationProto
from protocols.source_proto import SourceProto

_SOURCE_CLASS_NAME = "Source"
_DESTINATION_CLASS_NAME = "Destination"
_SOURCE_CLASS_FOLDER = "sources"
_DESTINATION_CLASS_FOLDER = "destinations"


class ValidationBuilder:
  """Builder class for validation DAGs."""

  @staticmethod
  def _get_validation_id(class_name: str) -> str:
    validation_id = f"validate_{class_name.lower()}"
    return validation_id

  def _get_folder_from_name(self, class_name: str) -> str:
    if class_name == _SOURCE_CLASS_NAME:
      return _SOURCE_CLASS_FOLDER
    elif class_name == _DESTINATION_CLASS_NAME:
      return _DESTINATION_CLASS_FOLDER
    else:
      raise ValueError(f"Unknown class name: {class_name}")

  def _instance_from_name(
      self, target_instance: str, class_name: str, config: Dict[str, Any]
  ) -> SourceProto | DestinationProto:
    root_folder = self._get_folder_from_name(class_name)
    module_name = f"{root_folder}.{target_instance}"
    module = importlib.import_module(module_name)
    class_ = getattr(module, class_name)
    return class_(config)

  def _build_validation_dag(
      self,
      target_class: str,
  ):
    """Builds a validation DAG for the given target_class."""
    validation_id = ValidationBuilder._get_validation_id(target_class)

    @dag(
        dag_id=validation_id,
        is_paused_upon_creation=False,
        start_date=datetime.datetime.now(),
        schedule_interval=None,
        render_template_as_native_obj=True,
    )
    def validation_dag():
      def validate(
          target_name: str,
          target_config: Dict[str, Any],
      ) -> None:
        """Performs the actual validation of source or destination."""
        target_instance = self._instance_from_name(
            target_name, target_class, target_config
        )

        # TODO(b/279079875): Pass destination fields to source validate to check schema in connection validation
        return asdict(target_instance.validate())

      PythonOperator(
          task_id=validation_id,
          op_kwargs={
              "target_name": "{{ dag_run.conf.get('target_name') }}",
              "target_config": "{{ dag_run.conf.get('target_config')}}",
          },
          python_callable=validate,
      )

    return validation_dag

  def register_validations(self):
    """Register source and destination validation DAGs."""
    for target_class in [_SOURCE_CLASS_NAME, _DESTINATION_CLASS_NAME]:
      validation_dag = self._build_validation_dag(target_class)
      validation_dag()


builder = ValidationBuilder()
builder.register_validations()
