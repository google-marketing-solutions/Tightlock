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
"""Definition of data models used by Tightlock application."""

import datetime
from typing import Any, Dict, Optional, Sequence

from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy_json import mutable_json_type
from sqlmodel import Column, DateTime, Field, SQLModel


class ConnectResponse(SQLModel):
  """Connect endpoint response."""

  version: str


class RunResult(SQLModel):
  """Result of a connection run."""

  successful_hits: int = 0
  failed_hits: int = 0
  error_messages: Sequence[str] = []
  dry_run: bool = False


class RunLog(SQLModel):
  """Full log of a connection run."""

  connection_name: str
  source_name: str
  destination_name: str
  schedule: str
  state: str
  run_at: Optional[datetime.datetime]
  run_type: str
  run_result: RunResult


class RunLogsResponse(SQLModel):
  """RunLogs endpoint response."""
  run_logs: Sequence[RunLog]
  total_entries: int


class ValidationResult(SQLModel):
  """Result of source or destination validation."""

  is_valid: bool
  messages: Sequence[str]


class ConfigValue(SQLModel):
  """Represents a generic value for configs (sources or destinations)."""

  value: Dict[str, Any]


class Connection(SQLModel):
  """Represents a configured connection.

  This model is part of the config and does not define a table for now.
  """

  name: str  # Connection name
  source: Dict[str, Any]  # Source
  destination: Dict[str, Any]  # Destination
  schedule: Optional[str] = None  # A cron expression or preset


class Config(SQLModel, table=True):
  """A Config that defines all connections, sources and destinations.

  For now, all lives in a JSON 'value' field.
  """

  __table_args__ = (UniqueConstraint("label"),)

  id: Optional[int] = Field(default=None, primary_key=True)
  create_date: datetime.datetime = Field(
      sa_column=Column(DateTime(timezone=True)),
      default_factory=datetime.datetime.now,
      nullable=False,
  )
  label: str
  # defined as below to avoid
  # https://amercader.net/blog/beware-of-json-fields-in-sqlalchemy/
  value: Dict[str, Any] = Field(default={},
                                sa_column=Column(
                                    mutable_json_type(dbtype=JSONB,
                                                      nested=True)))

  # Needed for Column(JSON)
  class Config:
    """Inner `Config` class is needed by sqlmodel.

    Same name as the parent class is a coincidence.
    """

    arbitrary_types_allowed = True


class Retries(SQLModel, table=True):
  """ Table of events to be retried.

  For now, all retry events lives in a JSON 'data' field.
  """

  __table_args__ = (UniqueConstraint("uuid"),)

  id: Optional[int] = Field(default=None, primary_key=True)
  connection_id: str
  uuid: str
  destination_type: str
  destination_folder: str
  destination_config: Dict[str,
                           Any] = Field(default={},
                                        sa_column=Column(
                                            mutable_json_type(dbtype=JSONB,
                                                              nested=True)))
  next_run: datetime.datetime = Field(
      sa_column=Column(DateTime(timezone=True)),
      default_factory=datetime.datetime.now,
      nullable=False,
  )
  retry_num: int
  # defined as below to avoid
  # https://amercader.net/blog/beware-of-json-fields-in-sqlalchemy/
  data: Dict[str, Any] = Field(default={},
                               sa_column=Column(
                                   mutable_json_type(dbtype=JSONB,
                                                     nested=True)))

  # Needed for Column(JSON)
  class Config:
    """Inner `Config` class is needed by sqlmodel.
    """

    arbitrary_types_allowed = True
