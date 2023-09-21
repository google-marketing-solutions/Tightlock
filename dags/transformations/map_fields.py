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

from schemas import ProtocolSchema

from pydantic import Field
from typing import Any, Dict, List, Mapping, Optional


class Transformation:
  """Implements field mappings between the Source and Destination fields."""

  def __init__(self, config: Dict[str, Any]):
    self.field_mappings: List[Dict[str, str]] = config["field_mappings"]

  def pre_transform(
      self,
      destination_fields: List[str]
  ) -> List[str]:
    self._validate_field_mappings(destination_fields)
    source_fields = []
    for destination_field in destination_fields:
      source_fields.append(
        self._get_source_field(destination_field)
      )
    return source_fields

  def post_transform(
      self,
      input_data: List[Mapping[str, Any]]
  ) -> List[Mapping[str, Any]]:
    for item in input_data:
      self._set_destination_fields(item)
    return input_data

  def _get_source_field(self, field: str) -> str:
    for mapping in self.field_mappings:
      if mapping["destination"] == field:
        return mapping["source"]
    return field

  def _validate_field_mappings(self, destination_fields: List[str]):
    mapped_destination_fields = [mapping["destination"] for mapping in self.field_mappings]
    for field in mapped_destination_fields:
      if field not in destination_fields:
        raise ValueError(
          f"Destination field {field} in mapping not found in destination fields."
        )

  def _set_destination_fields(self, item: Mapping[str, Any]):
    for mapping in self.field_mappings:
      source_field = mapping["source"]
      destination_field = mapping["destination"]
      if source_field in item:
        item[destination_field] = item[source_field]
        del item[source_field]

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    return ProtocolSchema(
        "map_fields",
        [
            ("field_mappings", List[Dict[str, str]], Field(
                description="The mappings for destination and source fields",
                validation="^[a-zA-Z0-9_]{1,1024}$")),
        ]
    )
