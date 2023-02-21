"""Integration tests for tightlock-api container."""

from urllib import parse


def test_initial_config(helpers):
  """Verifies if an initial config was provided at application startup."""
  request_session, api_url = helpers.get_container_client("tightlock-api")
  config = request_session.get(parse.urljoin(api_url, "api/v1/configs")).json()
  assert config[0]["label"] == "Initial Config"
