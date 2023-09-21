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

from hashlib import sha256

from protocols.transformation_proto import TransformationProto
from schemas import ProtocolSchema

from pydantic import Field
from typing import Any, Dict, List, Mapping, Optional
from utils import SchemaUtils


class Transformation:
    """Implements SHA256 Encoding value transformation."""
    def __init__(self, config: Dict[str, Any]):
        self.source_field_name = config["source_field_name"]

    def type(self) -> TransformationProto.Type:
       return TransformationProto.Type.PRE

    def pre_transform(
            self,
            fields: List[str]
    ) -> List[str]:
        return fields

    def post_transform(
            self,
            input_data: List[Mapping[str, Any]]
    ) -> List[Mapping[str, Any]]:
        for row_data in input_data:
            self._encode_field(row_data)

        return input_data

    def _encode_field(self, row_data: Mapping[str, Any]) -> None:
        if self.source_field_name not in row_data:
            raise ValueError(
                f"Transformation error:  Could not find field '{self.source_field_name}' to SHA encode."
            )

        value = row_data[self.source_field_name]
        if value:
            hash = sha256(value.encode('utf-8'))
            row_data[self.source_field_name] = hash.hexdigest()

    @staticmethod
    def schema() -> Optional[ProtocolSchema]:
        return ProtocolSchema(
            "sha256_encode",
            [
                ("source_field_name", str, Field(
                    description="The name of field in the source dataset to encode.",
                    validation="^[a-zA-Z0-9_]{1,1024}$")),
            ]
        )

# Might or might not need.
#   def validate(self) -> ValidationResult:
#     """Validates the provided config.

#     Returns:
#       A ValidationResult for the provided config.
#     """
#     ...






