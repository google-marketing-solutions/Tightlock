"""Integration tests for tightlock-api container."""

from urllib import parse


def test_initial_config(helpers):
  """Verifies if an initial config was provided at application startup."""
  request_session, api_url = helpers.get_tightlock_api_client()
  config = request_session.get(parse.urljoin(api_url, "api/v1/configs")).json()
  assert config[0]["label"] == "Initial Config"

def test_connect_authentication(helpers):
  """Verifies if connect/ endpoint is properly authenticated."""
  request_session, api_url = helpers.get_tightlock_api_client()
  request_session.headers.update({"X-API-Key": "fake-api-key"})
  response = request_session.post(parse.urljoin(api_url, "api/v1/connect"))
  assert response.status_code == 401
