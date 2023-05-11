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

import os

from fastapi import Depends, HTTPException
from fastapi.security import APIKeyHeader
from starlette import status

# Security checks
X_API_KEY = APIKeyHeader(name="X-API-Key")
TIGHTLOCK_API_KEY = os.environ.get("TIGHTLOCK_API_KEY")


def check_authentication_header(x_api_key: str = Depends(X_API_KEY)):
  """Takes the X-API-Key header and validates it."""
  if x_api_key != TIGHTLOCK_API_KEY:
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API Key",
    )
