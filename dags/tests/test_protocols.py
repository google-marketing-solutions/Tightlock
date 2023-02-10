"""Test adherence to Destination and Source protocols."""

from dags.protocols.destination_proto import DestinationProto
from dags.protocols.source_proto import SourceProto
from dags.utils import DagUtils

dag_utils = DagUtils()


def test_destinations_protocol():
  destinations = dag_utils.import_modules_from_folder("destinations")
  for destination in destinations:
    assert issubclass(destination.Destination, DestinationProto)

def test_sources_protocol():
  sources = dag_utils.import_modules_from_folder("sources")
  for source in sources:
    assert issubclass(source.Source, SourceProto)