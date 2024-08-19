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

"""Test utility methods."""

import json
import pytest
from unittest import mock

from dags.utils import DrillMixin
from dags.utils import TadauMixin

def test_parse_data():
  drill_mixin = DrillMixin()
  test_fields = ["str_field", "int_field"]
  test_rows = [("abc", 1), ("cde", 2)]
  result = drill_mixin._parse_data(test_fields, test_rows)
  assert {"str_field": "cde", "int_field": 2} in result


def mock_config(test_consent):
  return mock.mock_open(read_data=json.dumps({
      "opt_in": test_consent,
      "api_secret": "fake_secret",
      "measurement_id": "fake_id"
  }))


@pytest.mark.parametrize(
    "test_consent",
    [
        ("False"),
        ("false"),
        ("Whatever")
    ]
)
def test_tadau_no_consent(test_consent):
  """Asserts false or invalid consent config yields no Tadau object."""
  m = mock_config(test_consent)
  with mock.patch("builtins.open", m):
    t = TadauMixin()
    assert t._tadau is None


@pytest.mark.parametrize(
    "test_consent",
    [
        ("True"),
        ("true"),
    ]
)
def test_tadau_with_consent(test_consent):
  """Asserts valid consent config yields valid Tadau object."""
  m = mock_config(test_consent)
  with mock.patch("builtins.open", m):
    t = TadauMixin()
    assert t._tadau.opt_in
