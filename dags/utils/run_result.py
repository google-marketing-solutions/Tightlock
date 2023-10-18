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

from dataclasses import dataclass, field
from typing import Any, List, Mapping, Sequence

MAX_TRIES = 3


@dataclass
class RunResult:
  """Class for reporting the result of a DAG run."""

  successful_hits: int = 0
  failed_hits: int = 0
  error_messages: Sequence[str] = field(default_factory=lambda: [])
  retriable_events: Sequence[Mapping[str,
                                     Any]] = field(default_factory=lambda: [])
  dry_run: bool = False

  def __add__(self, other: "RunResult") -> "RunResult":
    sh = self.successful_hits + other.successful_hits
    fh = self.failed_hits + other.failed_hits
    em = self.error_messages + other.error_messages
    dr = self.dry_run or other.dry_run
    return RunResult(sh, fh, em, dr)
