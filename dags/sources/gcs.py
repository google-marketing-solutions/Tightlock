"""
 Copyright 2024 Google LLC

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

from typing import Any, Dict, List, Mapping, Optional, Sequence

from pydantic import Field
from utils import DrillMixin, ProtocolSchema, ValidationResult

_UNIQUE_ID_DEFAULT_NAME = "id"
_GCS_PLUGIN_NAME = "gcs"


class Source(DrillMixin):
  """Implements SourceProto protocol for Google Cloud Storage Buckets."""

  def __init__(self, config: Dict[str, Any]):
    self.config = config
    self.path = f"{_GCS_PLUGIN_NAME}.`{self.config['location']}`"
    self.unique_id = config.get("unique_id") or _UNIQUE_ID_DEFAULT_NAME

  def _get_gcs_storage(self):
    gcs_config = self._get_storage(_GCS_PLUGIN_NAME)
    config = gcs_config["config"]
    if not config:
      # use dfs plugin as template
      dfs_config = self._get_storage("dfs")
      gcs_config["config"] = dfs_config["config"]
      self._set_storage(_GCS_PLUGIN_NAME, gcs_config)

    return gcs_config

  def _set_gcs_storage(self):
    """Updates Drill GCS storage plugin (if config has changed).
    """
    gcs_config = self._get_gcs_storage()
    updates = {
        "connection": f"gs://{self.config['bucket_name']}",
        "gs_impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    }
    if "config" not in gcs_config["config"]:
      gcs_config["config"]["config"] = {}
    change_requested = False
    for obj, key, value in [
        (gcs_config["config"], "enabled", True),
        (gcs_config["config"], "connection", updates["connection"]),
        (gcs_config["config"]["config"], "fs.AbstractFileSystem.gs.impl", updates["gs_impl"]),
    ]:
      if self._validate_or_update_config_obj(obj, key, value):
        change_requested = True

    if change_requested:
      self._set_storage(_GCS_PLUGIN_NAME, gcs_config)

  def get_data(
      self,
      fields: Sequence[str],
      offset: int,
      limit: int,
      reusable_credentials: Optional[Sequence[Mapping[str, Any]]],
  ) -> List[Mapping[str, Any]]:
    # Make sure GCS plugin is configured with config from
    # this source before retrieving data
    self._set_gcs_storage()
    return self.get_drill_data(self.path, fields, offset, limit, self.unique_id)

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    return ProtocolSchema(
        "gcs",
        [
            ("bucket_name", str, Field(
                description="The name of the GCS bucket that contains your data.")),
            ("location", str, Field(
                description="The name of the GCS bucket folder that contains your data.")),
            ("unique_id", Optional[str], Field(
                description=f"Unique id column name to be used by gcs source engine. Defaults to '{_UNIQUE_ID_DEFAULT_NAME}' when nothing is provided.",
                default=_UNIQUE_ID_DEFAULT_NAME
            )),
        ]
    )

  def validate(self) -> ValidationResult:
    # Make sure GCS plugin is configured with config from
    # this source before validating source
    self._set_gcs_storage()
    return self.validate_drill(self.path, self.unique_id)
