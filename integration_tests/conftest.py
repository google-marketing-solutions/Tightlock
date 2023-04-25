"""Common functionalities for tests."""
import os
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

  def _get_container_client(self, container_name, auth=None, api_key=None):
    """Returns a Request session and the API url that can be used to communicate with the target container."""
    request_session = requests.Session()
    if auth:
      request_session.auth = auth
    if api_key:
      request_session.headers.update({"X-API-Key": api_key})
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    request_session.mount("http://", HTTPAdapter(max_retries=retries))

    service = self.container_getter.get(container_name).network_info[0]
    api_url = "http://%s:%s/" % (service.hostname, service.host_port)
    return request_session, api_url

  def get_tightlock_api_client(self):
    api_key = os.environ.get("TIGHTLOCK_API_KEY")
    return self._get_container_client("tightlock-api", api_key=api_key)

  def get_airflow_client(self):
    return self._get_container_client("airflow-webserver", auth=("airflow", "airflow"))

  def get_drill_client(self):
    return self._get_container_client("drill")


@pytest.fixture(scope="session", autouse=True)
def wait_for_api(session_scoped_container_getter):
  """Wait for Airflow and Drill to be ready before starting integration tests."""
  airflow_request_session, api_url = Helpers(
      session_scoped_container_getter
  ).get_airflow_client()
  drill_request_session, drill_url = Helpers(
    session_scoped_container_getter
  ).get_drill_client()
  assert airflow_request_session.get(parse.urljoin(api_url, "health"))
  assert drill_request_session.get(parse.urljoin(drill_url, "status"))


@pytest.fixture(scope="session")
def helpers(session_scoped_container_getter):
  return Helpers(session_scoped_container_getter)
