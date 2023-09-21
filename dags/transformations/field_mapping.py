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

import warnings
from protocols.transformation_proto import TransformationProto
from schemas import ProtocolSchema

from pydantic import Field
from typing import Any, Dict, List, Optional
from utils import SchemaUtils


class Transfomation:
    """Implements field mappings between the Source and Destination fields."""
    def __init__(self, config: Dict[str, Any]):
        self.field_mappings = config["field_mappings"]

    def type(self) -> TransformationProto.Type:
       return TransformationProto.Type.POST

    def pre_transform(
            self,
            dest_fields: List[str]
    ) -> List[str]:
        source_fields = []
        for dest_field in dest_fields:
            source_field = self.field_mapping(dest_field)
            source_fields.append(source_field)
        return source_fields

    def field_mapping(self, dest_field: str) -> str:
        try:
          return self.field_mappings.get(dest_field)
        except:
          raise KeyError(dest_field, " is not found in Config")

    @staticmethod
    def schema() -> Optional[ProtocolSchema]:
      return ProtocolSchema(
          "field_mapping",
          [
              ("field_mappings", Dict[str, str], Field(
                  description="The mappings for destination and source fields",
                  validation="^[a-zA-Z0-9_]{1,1024}$")),
          ]
      )




