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
        self.source_fields = config["source_fields"]
        self.dest_fields = config["dest_fields"]

    def type(self) -> TransformationProto.Type:
       return TransformationProto.Type.POST

    def pre_transform(
            self,
    ) -> List[Dict[str, str]]:
        pairs = []
        for i in range(len(self.dest_fields)):
            pair = self.field_mapping(self.source_fields[i],
                                      self.dest_fields[i])
            pairs.append(pair)
        return pairs

    def field_mapping(self, source_field: str,
                      dest_field: str) -> Dict[str, str]:
        if dest_field is None:
            raise KeyError("You are assigning ", source_field,
                           " to an empty destination field. Please create a "
                           "corresponding destination field "
                           "or remove source field: ", source_field)
        if source_field is None:
            print("You are creating a brand new destination field: ",
                  dest_field, ". Remember to fill the values in the next steps")
        return {dest_field: source_field}

    # @staticmethod
    # def schema() -> Optional[ProtocolSchema]:
    #     return




