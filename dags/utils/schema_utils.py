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
limitations under the License."""
"""Utility functions for DAGs."""

from typing import Any
from pydantic import BaseModel


class SchemaUtils:
  """A set of utility functions for defining schemas."""

  @staticmethod
  def key_value_type():

    class KeyValue(BaseModel):
      key: str
      value: str

    return KeyValue

  @staticmethod
  def raw_json_type():

    class RawJSON(BaseModel):
      value: str

    return RawJSON
