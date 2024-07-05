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

import os
import pytest
from dags.utils import DrillMixin 
from dags.utils import TadauMixin

def test_parse_data():
  drill_mixin = DrillMixin()
  test_fields = ["str_field", "int_field"]
  test_rows = [("abc", 1), ("cde", 2)]
  result = drill_mixin._parse_data(test_fields, test_rows)
  assert {"str_field": "cde", "int_field": 2} in result

@pytest.mark.parametrize(
  "test_consent",
  [
      ("False"),
      ("false"),
      ("Whatever")
  ]
)
def test_tadau_no_consent(test_consent):
    # remove existing config, if any. Alternativaley, mock path and pretend there is no files there
    os.remove("PATH")
    # modify environment with test env. See if there is an alternative to temproarily modify env for the purpose of the test
    os.environ["USAGE_COLLECTION_ALLOWED"] = test_consent
    print(test_consent)
    t = TadauMixin()
    assert t.tadau is None

    # TODO maybe cleanup since this may modify the env 

@pytest.mark.parametrize(
  "test_consent",
  [
      ("True"),
      ("true"),
      (True),
  ]
)
def test_tadau_with_consent(test_consent):
    pass
