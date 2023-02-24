
"""Integration tests for drill container."""

from urllib import parse


def test_sample_data(helpers):
  """Verifies if sample data files are accessible by Drill."""
  request_session, api_url = helpers.get_container_client('drill')
  test_payload = {
      'queryType': 'SQL',
      'query': 'SELECT * FROM dfs.`data/integration_test.csvh`',
      'autoLimit': 100
  }
  result = request_session.post(parse.urljoin(api_url, 'query.json'),
                                json=test_payload).json()
  assert result['queryState'] == 'COMPLETED'
