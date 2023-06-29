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

"""Test adherence to Destination and Source protocols."""
import inspect

from protocols.destination_proto import DestinationProto
from protocols.source_proto import SourceProto
from utils import DagUtils

dag_utils = DagUtils()

def base_test_protocol(folder_name: str, protocol: SourceProto | DestinationProto):
  modules = dag_utils.import_modules_from_folder(folder_name)
  for module in modules:
    target_class = "Source" if protocol.__name__ == "SourceProto" else "Destination" if protocol.__name__ == "DestinationProto" else None
    if not target_class:
      raise ValueError(f"Provided protocol {protocol} not supported.")
    implementation = getattr(module, target_class)
    # Assert weak requirement that protocol methods are implemented with same name
    assert issubclass(implementation, protocol)
    # Assert strong requirement that protocol methods are implemented with the same signature
    target_methods = inspect.getmembers(implementation, inspect.isfunction)
    protocol_methods = inspect.getmembers(protocol, inspect.isfunction)
    for required_name, required_method in protocol_methods:
      for target_name, target_method in target_methods:
        if required_name == target_name:
          assert inspect.signature(required_method) == inspect.signature(target_method)

def test_destinations_protocol():
  base_test_protocol("destinations", DestinationProto)

def test_sources_protocol():
  base_test_protocol("sources", SourceProto)
