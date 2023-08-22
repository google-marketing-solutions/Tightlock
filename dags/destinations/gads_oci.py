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
from utils import ProtocolSchema, RunResult, SchemaUtils, ValidationResult

_OAUTH2_TOKEN_URL = 'https://www.googleapis.com/oauth2/v3/token'
_REQUIRED_CREDENTIALS = ["client_id", "client_secret", "refresh_token", "developer_token", "login_customer_id"]

_GOOGLE_ADS_API_BASE_URL = 'https://googleads.googleapis.com'

_API_VERSION = '14'

_OCI_ENDPOINT = (
    GOOGLE_ADS_API_BASE_URL
    + '/{api_version}/customers/{customer_id}:uploadClickConversions'
)

class Destination:
  """Implements DestinationProto protocol for Google Ads OCI."""

  def __init__(self, config: Dict[str, Any]) -> None:
    """ Initializes Google Ads OCI Destination Class. 

    Args:
      config: Configuration object to hold environment variables
    """

    self._config = config  # Keeping a reference for convenience.
    self._client_id = config.get("client_id", "")
    self._client_secret = config.get("client_secret", "")
    self._refresh_token = config.get("refresh_token", "")
    self._developer_token = config.get("developer_token", "")
    self._login_customer_id = config.get("login_customer_id", "")
    self._access_token = None

    self._debug = config.get("debug", False)
    self._validate_credentials()
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
    return ProtocolSchema(
        "GA4MP",
        [
            ("api_secret", str, Field(
                description="An API SECRET generated in the Google Analytics UI.")),
            ("event_type", PayloadTypes, Field(
                description="GA4 client type.",
                validation="gtag|firebase")),
            ("measurement_id", str, Field(
                condition_field="event_type",
                condition_target="gtag",
                description="The measurement ID associated with a stream. Found in the Google Analytics UI.")),
            ("firebase_app_id", str, Field(
                condition_field="event_type",
                condition_target="firebase",
                description="The Firebase App ID. The identifier for a Firebase app. Found in the Firebase console.",
                validation="^[0-9a-fA-F]{32}$")),
            ("non_personalized_ads", Optional[bool], Field(
                default=False,
                description="Set to true to indicate these events should not be used for personalized ads.")),
            ("debug", Optional[bool], Field(
                default=False,
                description="Dry-run (validation mode).")),
            ("user_properties", Optional[List[_KEY_VALUE_TYPE]], Field(
                default=None,
                description="The user properties for the measurement.")),
        ]
    )

  def fields(self) -> Sequence[str]:
    if self.payload_type == PayloadTypes.FIREBASE.value:
      id_column_name = _FIREBASE_ID_COLUMN
    else:
      id_column_name = _GTAG_ID_COLUMN
    return [id_column_name] + _GA_REFERENCE_PARAMS + [
        "user_id",
        "event_name",
        "timestamp_micros"
    ]

  def batch_size(self) -> int:
    return 10000

  def validate(self) -> ValidationResult:
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

def _validate_credentials(self) -> None:
    """Validate credentials.

    Raises:
      DataConnectorValueError: If any credentials are missing from config.
    """
    for credential in _REQUIRED_CREDENTIALS:
      if credential not in self._config:
        raise errors.DataOutConnectorValueError(
            f"Missing API secret in config: {credential}"
        )

def _get_http_header(self) -> dict[str, str]:
    """Get the Authorization HTTP header.

    Returns:
      The authorization HTTP header.
    """
    if not self.access_token:
      self.access_token = _refresh_access_token()
    return {
        'authorization': f'Bearer {self._access_token}',
        'developer-token': self._developer_token,
        'login-customer-id': str(self._login_customer_id),
    }

def refresh_access_token(self) -> str:
  """Requests an OAUTH2 access token.

  Returns:
    An access token string, or empty string if the request failed.
  """
  payload = {
      'grant_type': 'refresh_token',
      'client_id': self._client_id,
      'client_secret': self._client_secret,
      'refresh_token': self._refresh_token,
  }

  response = requests.post(_OAUTH2_TOKEN_URL, params=payload)
  response.raise_for_status()
  data = response.json()

  return data.get('access_token', '')