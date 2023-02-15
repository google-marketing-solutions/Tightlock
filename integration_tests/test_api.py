import pytest
import requests
from urllib.parse import urljoin
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

pytest_plugins = ["docker_compose"]

# Invoking this fixture: 'function_scoped_container_getter' starts all services
@pytest.fixture(scope="function")
def wait_for_api(function_scoped_container_getter):
  """Wait for the tightlock-api to become responsive."""
  request_session = requests.Session()
  retries = Retry(total=5,
                  backoff_factor=0.1,
                  status_forcelist=[500, 502, 503, 504])
  request_session.mount('http://', HTTPAdapter(max_retries=retries))

  service = function_scoped_container_getter.get("tightlock-api").network_info[0]
  api_url = "http://%s:%s/" % (service.hostname, service.host_port)
  assert request_session.get(urljoin(api_url, "api/v1/configs"))
  return request_session, api_url


def test_initial_config(wait_for_api):
  """Verifies if an initial config was provided at application startup."""
  request_session, api_url = wait_for_api
  config = request_session.get(urljoin(api_url, "api/v1/configs")).json()
  assert config[0]["label"] == "Initial Config"