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


class Source(DrillMixin):
  """Implements SourceProto protocol for S3 Buckets."""

  def __init__(self, config: Dict[str, Any]):
    self.config = config
    self.path = f"s3.`{self.config['location']}`"
    self.unique_id = config.get("unique_id") or _UNIQUE_ID_DEFAULT_NAME

  def _set_s3_storage(self):
    """Updates Drill s3 storage plugin (if config has changed).
    """
    s3_plugin_name = "s3"
    s3_config = self._get_storage(s3_plugin_name)
    secret_key = self.config.get("secret_key")
    access_key = self.config.get("access_key")
    updates = {
        "connection": f"s3a://{self.config['bucket_name']}",
        "secret_key": secret_key,
        "access_key": access_key,
        "default_provider": "",
        "disable_cache": "true"
    }
    if not secret_key or not access_key:
      updates["default_provider"] = (
          "com.amazonaws.auth.InstanceProfileCredentialsProvider"
      )

    if "config" not in s3_config["config"]:
      s3_config["config"]["config"] = {}
    change_requested = False
    for obj, key, value in [
        (s3_config["config"], "enabled", True),
        (s3_config["config"], "connection", updates["connection"]),
        (s3_config["config"]["config"], "fs.s3a.secret.key", updates["secret_key"]),
        (s3_config["config"]["config"], "fs.s3a.access.key", updates["access_key"]),
        (s3_config["config"]["config"], "fs.s3a.aws.credentials.provider", updates["default_provider"]),
        (s3_config["config"]["config"], "fs.s3a.impl.disable.cache", updates["disable_cache"])
    ]:
      if self._validate_or_update_config_obj(obj, key, value):
        change_requested = True

    if change_requested:
      self._set_storage(s3_plugin_name, s3_config)

  def get_data(
      self,
      fields: Sequence[str],
      offset: int,
      limit: int,
      reusable_credentials: Optional[Sequence[Mapping[str, Any]]],
  ) -> List[Mapping[str, Any]]:
    # Make sure S3 plugin is configured with config from
    # this source before retrieving data
    self._set_s3_storage()

    return self.get_drill_data(self.path, fields, offset, limit, self.unique_id)

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    return ProtocolSchema(
        "s3",
        [
            ("bucket_name", str, Field(
                description="The name of the S3 bucket that contains your data.")),
            ("location", str, Field(
                description="The name of the S3 bucket folder that contains your data.")),
            ("secret_key", Optional[str], Field(
                description="Optional AWS secret key (only needed when running Tightlock on a separate cloud environment).",
                default=None)),
            ("access_key", Optional[str], Field(
                description="Optional AWS access key (only needed when running Tightlock on a separate cloud environment).",
                default=None)),
            ("unique_id", Optional[str], Field(
                description=f"Unique id column name to be used by s3 source engine. Defaults to '{_UNIQUE_ID_DEFAULT_NAME}' when nothing is provided.",
                default=_UNIQUE_ID_DEFAULT_NAME
            )),
        ]
    )

  def validate(self) -> ValidationResult:
    # Make sure S3 plugin is configured with config from
    # this source before validating source
    self._set_s3_storage()
    return self.validate_drill(self.path, self.unique_id)
