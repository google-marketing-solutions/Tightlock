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

"""Common functionalities for tests."""
import os
from urllib import parse

import pytest
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tenacity import retry, wait_exponential, stop_after_attempt

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
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[404, 500, 502, 503, 504])
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
@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
def wait_for_api(session_scoped_container_getter):
  """Wait for Airflow and Drill to be ready before starting integration tests."""
  airflow_request_session, airflow_url = Helpers(
      session_scoped_container_getter
  ).get_airflow_client()
  drill_request_session, drill_url = Helpers(
    session_scoped_container_getter
  ).get_drill_client()
  api_request_session, api_url = Helpers(
    session_scoped_container_getter
  ).get_tightlock_api_client()

  # assert Airflow health
  airflow_health = airflow_request_session.get(parse.urljoin(airflow_url, "health")).json().get("metadatabase").get("status")
  assert airflow_health == "healthy"
  # assert Drill health
  drill_health_status_code = drill_request_session.get(parse.urljoin(drill_url, "status")).status_code
  assert drill_health_status_code == 200
  # assert API health
  api_health_status_code = api_request_session.post(parse.urljoin(api_url, "api/v1/connect")).status_code
  assert api_health_status_code == 200


@pytest.fixture(scope="session")
def helpers(session_scoped_container_getter):
  return Helpers(session_scoped_container_getter)
