"""Common functionalities for tests."""

from urllib import parse
import pytest
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

pytest_plugins = ["docker_compose"]


class Helpers:
  """Helper methods for integration tests."""

  def __init__(self, container_getter):
    self.container_getter = container_getter

  def get_container_client(self, container_name, auth=None):
    """Returns a Request session and the API url that can be used to communicate with the target container."""
    request_session = requests.Session()
    if auth:
      request_session.auth = auth
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    request_session.mount("http://", HTTPAdapter(max_retries=retries))

    service = self.container_getter.get(container_name).network_info[0]
    api_url = "http://%s:%s/" % (service.hostname, service.host_port)
    return request_session, api_url


@pytest.fixture(scope="session", autouse=True)
def wait_for_api(session_scoped_container_getter):
  """Wait for Airflow to be ready before starting integration tests."""
  airflow_request_session, api_url = Helpers(
      session_scoped_container_getter
  ).get_container_client("airflow-webserver")
  assert airflow_request_session.get(parse.urljoin(api_url, "health"))


@pytest.fixture(scope="session")
def helpers(session_scoped_container_getter):
  return Helpers(session_scoped_container_getter)
