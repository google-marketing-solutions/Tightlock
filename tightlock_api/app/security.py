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
