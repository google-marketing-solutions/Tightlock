"""Test adherence to Destination and Source protocols."""
from protocols.destination_proto import DestinationProto
from protocols.source_proto import SourceProto
from utils import DagUtils
import inspect


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
        # TODO(b/278795989): Check __init__ signature when Airflow image is bumped to Python 3.11
        if required_name == target_name and target_name != "__init__":
          assert inspect.signature(required_method) == inspect.signature(target_method)

def test_destinations_protocol():
  base_test_protocol("destinations", DestinationProto)

def test_sources_protocol():
  base_test_protocol("sources", SourceProto)
