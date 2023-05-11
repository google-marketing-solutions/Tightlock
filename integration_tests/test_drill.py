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


"""Integration tests for drill container."""

from urllib import parse


def test_sample_data(helpers):
  """Verifies if sample data files are accessible by Drill."""
  request_session, api_url = helpers.get_drill_client()
  test_payload = {
      'queryType': 'SQL',
      'query': 'SELECT * FROM dfs.`data/integration_test.csvh`',
      'autoLimit': 100
  }
  result = request_session.post(parse.urljoin(api_url, 'query.json'),
                                json=test_payload).json()
  assert result['queryState'] == 'COMPLETED'
