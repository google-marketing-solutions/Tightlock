
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

import pytest
from schemas import reduce_schemas
from utils import ProtocolSchema


@pytest.mark.parametrize(
    "test_schemas,expected_result",
    [([], None),
     ([int], int),
     ([int, float, str], int | float | str)
    ]
)
def test_reduce_schemas(test_schemas, expected_result):
  assert reduce_schemas(test_schemas) == expected_result
