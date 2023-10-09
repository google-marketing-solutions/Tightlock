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

import importlib
import os
import pathlib
import sys
import traceback
from dataclasses import dataclass, field
from typing import Any, List, Mapping, Sequence, Tuple

from airflow.providers.apache.drill.hooks.drill import DrillHook
from pydantic import BaseModel
from google.ads.googleads.client import GoogleAdsClient

_TABLE_ALIAS = "t"
_DEFAULT_GOOGLE_ADS_API_VERSION = "v14"

_REQUIRED_GOOGLE_ADS_CREDENTIALS = frozenset([
  "client_id",
  "client_secret",
  "developer_token",
  "login_customer_id",
  "refresh_token"])


@dataclass
class ProtocolSchema:
  """Class that defines the schema of a source or destination protocol."""

  class_name: str
  fields: Sequence[Tuple[str, type] | Tuple[str, type, field]]


@dataclass
class ValidationResult:
  """Class for reporting of validation results."""

  is_valid: bool
  messages: Sequence[str]


@dataclass
class RunResult:
  """Class for reporting the result of a DAG run."""

  successful_hits: int = 0
  failed_hits: int = 0
  error_messages: Sequence[str] = field(default_factory=lambda: [])
  retriable_events: Sequence[Mapping[str, Any]] = field(default_factory=lambda: [])
  dry_run: bool = False

  def __add__(self, other: "RunResult") -> "RunResult":
    sh = self.successful_hits + other.successful_hits
    fh = self.failed_hits + other.failed_hits
    em = self.error_messages + other.error_messages
    dr = self.dry_run or other.dry_run
    return RunResult(sh, fh, em, dr)


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


class DagUtils:
  """A set of utility functions for DAGs."""

  def import_modules_from_folder(self, folder_name: str):
    """Import all modules from a given folder."""
    modules = []
    dags_path = f"airflow/dags/{folder_name}"
    folder_path = pathlib.Path().resolve().parent / dags_path
    for filename in os.listdir(folder_path):
      if os.path.isfile(folder_path / filename) and filename != "__init__.py":
        module_name, _ = filename.split(".py")
        module_path = os.path.join(folder_path, filename)
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        modules.append(module)
    return modules


class GoogleAdsUtils:
  """Utility functions for Google Ads connectors."""

  def validate_google_ads_config(self, config: dict[str, Any]) -> ValidationResult:
    """Validates the provided config can build a Google Ads client.

    Args:
      config: The Tightlock config file.

    Returns:
      A ValidationResult for the provided config.
    """
    missing_fields = []
    for credential in _REQUIRED_GOOGLE_ADS_CREDENTIALS:
      if not config.get(credential, ""):
        missing_fields.append(credential)

    if missing_fields:
      error_msg = (
        "Config requires the following fields to be set: "
        f"{', '.join(missing_fields)}")
      return ValidationResult(False, [error_msg])

    return ValidationResult(True, [])

  def build_google_ads_client(
      self,
      config: dict[str, Any],
      version: str=_DEFAULT_GOOGLE_ADS_API_VERSION) -> GoogleAdsClient:
    """Generate Google Ads Client.

    Requires the following to be stored in config:
    - client_id
    - client_secret
    - developer_token
    - login_customer_id
    - refresh_token

    Args:
      config: The Tightlock config file.
      version: (Optional) Version number for Google Ads API prefixed with v.

    Returns: Instance of GoogleAdsClient
    """
    credentials = {}

    for credential in _REQUIRED_GOOGLE_ADS_CREDENTIALS:
      credentials[credential] = config.get(credential, "")

    credentials["use_proto_plus"] = True

    return GoogleAdsClient.load_from_dict(
      config_dict=credentials, version=version)


class DrillMixin:
  """A Drill mixin that provides a get_drill_data wrapper for other classes that use drill."""

  def _parse_data(self, fields, rows: List[Tuple[str, ...]]) -> List[Mapping[str, Any]]:
    """Parses data and transforms it into a list of dictionaries."""
    events = []
    for event in rows:
      event_dict = {}
      # relies on Drill preserving the order of fields provided in the query
      for i, field in enumerate(fields):
        event_dict[field] = event[i]
      events.append(event_dict)
    return events

  def get_drill_data(
      self, from_target: Sequence[str], fields: Sequence[str], offset: int, limit: int
  ) -> List[Mapping[str, Any]]:
    drill_conn = DrillHook().get_conn()
    cursor = drill_conn.cursor()
    table_alias = _TABLE_ALIAS
    fields_str = ",".join([f"{table_alias}.{field}" for field in fields])
    query = (
        f"SELECT {fields_str}"
        f" FROM {from_target} as {table_alias}"
        f" LIMIT {limit} OFFSET {offset}"
    )
    try:
      cursor.execute(query)
      results = self._parse_data(fields, cursor.fetchall())
    except RuntimeError:
      # Return an empty list when an empty cursor is fetched
      results = []
    return results

  def validate_drill(self, path: str) -> ValidationResult:
    drill_conn = DrillHook().get_conn()
    cursor = drill_conn.cursor()
    query = f"SELECT COUNT(1) FROM {path}"
    try:
      cursor.execute(query)
    except Exception:  # pylint: disable=broad-except
      print(f"Drill validation error: {traceback.format_exc()}")
      return ValidationResult(False, [f"Invalid location: {path}"])
    return ValidationResult(True, [])


# TODO(blevitan): DRY. (Stole this from `tightlock_api/db.py``)
from sqlalchemy.orm import sessionmaker
from sqlmodel import create_engine
from sqlmodel.ext.asyncio.session import AsyncEngine
from sqlmodel.ext.asyncio.session import AsyncSession

engine = AsyncEngine(
    create_engine(
        # TODO(blevitan): Definitely need to change this hardcoding.
        'postgresql+asyncpg://tightlock:tightlock@postgres/tightlock',
        echo=True,
        future=True))


async def get_session() -> AsyncSession:
  async_session = sessionmaker(engine,
                               class_=AsyncSession,
                               expire_on_commit=False)
  async with async_session() as session:
    yield session
