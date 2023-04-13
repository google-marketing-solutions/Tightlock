"""Test utility methods."""

from dags.utils import DrillMixin 

def test_parse_data():
  drill_mixin = DrillMixin()
  test_fields = ["str_field", "int_field"]
  test_rows = [("abc", 1), ("cde", 2)]
  result = drill_mixin._parse_data(test_fields, test_rows)
  assert {"str_field": "cde", "int_field": 2} in result
