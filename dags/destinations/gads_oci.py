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

"""Google Ads OCI destination implementation."""

# pylint: disable=raise-missing-from

import datetime
import enum
import json
import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import errors
import immutabledict
import requests
from pydantic import Field
from utils import DagUtils, ProtocolSchema, RunResult, SchemaUtils, ValidationResult

_BATCH_SIZE = 10000

_REQUIRED_FIELDS = [
  "customer_id",
  "conversion_action_id",
  "conversion_date_time",
  "conversion_value",
]


class Destination:
  """Implements DestinationProto protocol for Google Ads OCI."""

  def __init__(self, config: Dict[str, Any]) -> None:
    """ Initializes Google Ads OCI Destination Class.

    Args:
      config: Configuration object to hold environment variables
    """
    self._config = config  # Keeping a reference for convenience.
    self.google_ads_client = DagUtils().build_google_ads_client(self._config)
    self._debug = config.get("debug", False)

    print("Initialized Google Ads OCI Destination class.")

  def send_data(
      self, input_data: List[Mapping[str, Any]], dry_run: bool
  ) -> Optional[RunResult]:
    """Builds payload ans sends data to Google Ads API."""
    valid_events, invalid_indices_and_errors = self._get_valid_and_invalid_events(
        input_data
    )

    if not dry_run:
      for valid_event in valid_events:
        try:
          event = valid_event[1]
          self._send_payload(event)
        except (
            errors.DataOutConnectorSendUnsuccessfulError,
            errors.DataOutConnectorValueError,
        ) as error:
          index = valid_event[0]
          invalid_indices_and_errors.append((index, error.error_num))
    else:
      print(
          "Dry-Run: Events will be validated agains the debug endpoint and will not be actually sent."
      )

    print(f"Valid events: {valid_events}")
    print(f"Invalid events: {invalid_indices_and_errors}")

    for invalid_event in invalid_indices_and_errors:
      event_index = invalid_event[0]
      error_num = invalid_event[1]
      # TODO(b/272258038): TBD What to do with invalid events data.
      print(f"event_index: {event_index}; error_num: {error_num}")

    run_result = RunResult(
        successful_hits=len(valid_events),
        failed_hits=len(invalid_indices_and_errors),
        error_messages=[str(error[1]) for error in invalid_indices_and_errors],
        dry_run=dry_run,
    )

    return run_result

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    """Returns the required metadata for this destination config.

    Returns:
      An optional ProtocolSchema object that defines the
      required and optional metadata used by the implementation
      of this protocol.
    """
    return ProtocolSchema(
      "GADS_OCI",
      [
        ("customer_id", str, Field(description="The Google Ads customer ID.")),
        ("conversion_action_id", str, Field(
          description="The conversion action ID.")),
        ("conversion_date_time", str, Field(description=(
          "The date and time of the conversion (should be after the click "
          "time). The format is 'yyyy-mm-dd hh:mm:ss+|-hh:mm, e.g. "
          "'2019-01-01 12:32:45-08:00'"))),
        ("conversion_value", str, Field(description="The conversion value.")),
        ("conversion_custom_variable_id", Optional[str], Field(default=None,
        description=("The ID of the conversion custom variable to associate "
        "with the upload."))),
        ("conversion_custom_variable_value", Optional[str], Field(
          default=None, description=("The value of the conversion custom "
          "variable to associate with the upload."))),
        ("gclid", Optional[str], default=None, Field(description=(
          "The Google Click Identifier (gclid) which should be newer than "
          "the number of days set on the conversion window of the conversion "
          "action. Only one of either a gclid, WBRAID, or GBRAID identifier can "
          "be passed into this example. See the following for more details: "
          "https://developers.google.com/google-ads/api/docs/conversions/upload-clicks"))),
        ("gbraid", Optional[str], default=None, Field(description=(
          "The GBRAID identifier for an iOS app conversion. Only one of "
          "either a gclid, WBRAID, or GBRAID identifier can be passed into this "
          "example. See the following for more details: "
          "https://developers.google.com/google-ads/api/docs/conversions/upload-clicks"))),
        ("wbraid", Optional[str], Field(default=None, description=(
          "The WBRAID identifier for an iOS app conversion. Only one of "
          "either a gclid, WBRAID, or GBRAID identifier can be passed into this "
          "example. See the following for more details: "
          "https://developers.google.com/google-ads/api/docs/conversions/upload-clicks"))),
      ]
    )

  def fields(self) -> Sequence[str]:
    """Lists required fields for the destination input data.

    Returns:
      A sequence of fields.
    """
    return _REQUIRED_FIELDS

  def batch_size(self) -> int:
    """Returns the required batch_size for the underlying destination API.

    Returns:
      An int representing the batch_size.
    """
    return _BATCH_SIZE

  def validate(self) -> ValidationResult:
    """Validates the provided config.

    Returns:
      A ValidationResult for the provided config.
    """
    timestamp_micros = int(datetime.datetime.now().timestamp() * 1e6)
    payload = {
        "timestamp_micros": timestamp_micros,
        "non_personalized_ads": False,
        "events": [],
        "validationBehavior": "ENFORCE_RECOMMENDATIONS",
    }
    if self.payload_type == PayloadTypes.GTAG.value:
      payload["client_id"] = "validation_client_id"
    else:
      payload[
          "app_instance_id"
      ] = "cccccccccccccccccccccccccccccccc"  # 32 digit app_instance_id
    validation_response = self._send_validate_request(payload).json()
    validation_messages = validation_response["validationMessages"]
    if len(validation_messages) < 1:
      return ValidationResult(True, [])
    else:
      return ValidationResult(False, validation_messages)
