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

from dags.utils import DrillMixin 

def test_parse_data():
  drill_mixin = DrillMixin()
  test_fields = ["str_field", "int_field"]
  test_rows = [("abc", 1), ("cde", 2)]
  result = drill_mixin._parse_data(test_fields, test_rows)
  assert {"str_field": "cde", "int_field": 2} in result
