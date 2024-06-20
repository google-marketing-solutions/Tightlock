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

"""CM 360 OCI destination implementation."""

# pylint: disable=raise-missing-from

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import googleapiclient
from googleapiclient import discovery
import google_auth_httplib2
import google.oauth2.credentials

import errors
from pydantic import Field
from utils import ProtocolSchema, RunResult, ValidationResult

_TIMESTAMP_MICROS_FIELD = "timestampMicros"

CM_CONVERSION_FIELDS = [
  _TIMESTAMP_MICROS_FIELD,
  "floodlightConfigurationId",
  "floodlightActivityId",
  "encryptedUserId",
  "mobileDeviceId",
  "value",
  "quantity",
  "ordinal",
  "limitAdTracking",
  "childDirectedTreatment",
  "gclid",
  "nonPersonalizedAd",
  "treatmentForUnderage",
  "matchId",
  "dclid",
  "impressionId",
  "userIdentifiers",
  "kind",
] + ["u" + str(i) for i in range(1, 101)]  # Add fields for the supported number of custom Floodlight variables (100).

CM_REQUIRED_CONVERSIONS_FIELDS = [
  _TIMESTAMP_MICROS_FIELD,
  "floodlightConfigurationId",
  "floodlightActivityId",
  "value",
  "quantity",
  "ordinal",
]

CM_MUTUALLY_EXCLUSIVE_CONVERSIONS_FIELDS = [
  "encryptedUserId",
  "mobileDeviceId",
  "gclid",
  "matchId",
  "dclid",
  "impressionId",
]

CM_CREDENTIALS = [
  "access_token",
  "refresh_token",
  "client_id",
  "client_secret",
]

class Destination:
  """Implements DestinationProto protocol for Campaign Manager Offline Conversion Import."""

  def __init__(self, config: Dict[str, Any]):
    self.config = config  # Keeping a reference for convenience.
    self.profile_id = config.get("profile_id")
    self.credentials = {}

    for credential in CM_CREDENTIALS:
      self.credentials[credential] = config.get(credential, "")

    self.encryption_info = {}
    self.encryption_info["encryptionEntityType"] = (
        config.get("encryptionEntityType", "")
    )
    self.encryption_info["encryptionEntityId"] = (
        config.get("encryptionEntityId", "")
    )
    self.encryption_info["encryptionSource"] = (
        config.get("encryptionSource", "")
    )
    self.validate()
    self._validate_credentials()

    # Authenticate using the supplied user account credentials
    self.http = self.authenticate_using_user_account()

  def authenticate_using_user_account(self):
    """Authorizes an httplib2.Http instance using user account credentials."""

    credentials = google.oauth2.credentials.Credentials(
        self.credentials["access_token"],
        refresh_token=self.credentials["refresh_token"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=self.credentials["client_id"],
        client_secret=self.credentials["client_secret"]
    )

    # Use the credentials to authorize an httplib2.Http instance.
    http = google_auth_httplib2.AuthorizedHttp(credentials)

    return http

  def _validate_credentials(self) -> None:
    """Validate credentials.

    Raises:
      Exception: If credential combination does not meet criteria.
    """
    for credential in CM_CREDENTIALS:
      if not self.credentials[credential]:
        raise errors.DataOutConnectorValueError(
            f"Missing {credential} in config: {self.config}"
        )

  def _parse_timestamp_micros(self, conversion: Dict[str, Any]):
    t = conversion.get(_TIMESTAMP_MICROS_FIELD)
    if t:
      timestamp_micros = int(t) if t.isdigit() else None
      return timestamp_micros
    return None

  def _send_payload(self, payload: Dict[str, Any]) -> None:
    """Sends conversions payload to CM360 via CM API.

    Args:
      payload: Parameters containing required data for conversion tracking.

    Returns:
      results: Includes request body, status_code, error_msg, response body and
      debug flag.
    """

    # Construct a service object via the discovery service.
    service = discovery.build("dfareporting", "v4", http=self.http)

    try:
      request = service.conversions().batchinsert(
          profileId=self.profile_id,
          body=payload
      )

      response = request.execute()
      # Success is to be considered between 200 and 299:
      # https://developer.mozilla.org/en-US/docs/Web/HTTP/Status
      if response["hasFailures"]:
        error_num = errors.ErrorNameIDMap.NON_RETRIABLE_ERROR_EVENT_NOT_SENT
        status_errors = []
        for status in response["status"]:
          if "errors" in status:
            status_errors.append(status["errors"])
            print(status["errors"])
        raise errors.DataOutConnectorSendUnsuccessfulError(
            msg=f"Sending payload to CM360 did not complete successfully: {status_errors}",
            error_num=error_num,
        )

    except googleapiclient.errors.HttpError as http_error:
      if http_error.resp.status >= 400 and http_error.resp.status < 500:
        error_num = errors.ErrorNameIDMap.NON_RETRIABLE_ERROR_EVENT_NOT_SENT
      else:
        error_num = errors.ErrorNameIDMap.RETRIABLE_CM360_HOOK_ERROR_HTTP_ERROR
      raise errors.DataOutConnectorSendUnsuccessfulError(
          msg=f"Sending payload to CM360 did not complete successfully: {http_error}",
          error_num=error_num,
      )

  def send_data(self, input_data: List[Mapping[str, Any]], dry_run: bool) -> Optional[RunResult]:
    """Builds payload and sends data to CM360 API."""

    valid_conversion_tuples = []
    invalid_conversion_tuples = []
    http_error = None
    gclid_based_conversions = False

    for i, entry in enumerate(input_data):
      conversion = {}
      for conversion_field in (CM_REQUIRED_CONVERSIONS_FIELDS + CM_CONVERSION_FIELDS + CM_MUTUALLY_EXCLUSIVE_CONVERSIONS_FIELDS):
        if conversion_field == _TIMESTAMP_MICROS_FIELD:
          timestamp_micros = self._parse_timestamp_micros(entry)
          if timestamp_micros:
            conversion[conversion_field] = timestamp_micros
          else:
            conversion[conversion_field] = ""
        else:
          value = entry.get(conversion_field)
          if value:
            conversion[conversion_field] = value

            # if gclid value is valid, mark the whole batch as gclid_based
            if conversion_field == "gclid":
              gclid_based_conversions = True
       
      is_valid, error_message = self._validate_conversion(conversion)
      if is_valid:
        valid_conversion_tuples.append((i, conversion))
      else:
        invalid_conversion_tuples.append((i, error_message))

    if valid_conversion_tuples:
      payload = {}

      # Does not include encryptionInfo object if batch is gclid-based
      if not gclid_based_conversions:
        payload["encryptionInfo"] = self.encryption_info

      payload["conversions"] = [
          conversion_tuple[1]
          for conversion_tuple in valid_conversion_tuples
      ]

      if not dry_run:
        try:
          self._send_payload(payload)
        except (
            errors.DataOutConnectorSendUnsuccessfulError,
        ) as error:
          http_error = error.msg
      else:
        print(
            "Dry-Run: CM conversions event will not be sent."
        )

    if http_error:
      invalid_conversion_tuples += [(conversion_tuple[0], http_error) for conversion_tuple in valid_conversion_tuples]
      valid_conversion_tuples = []

    print(f"Sent entries: {len(valid_conversion_tuples)}")
    print(f"{invalid_conversion_tuples=}")
    print(f"Invalid entries: {len(invalid_conversion_tuples)}")

    run_result = RunResult(
        successful_hits=len(valid_conversion_tuples),
        failed_hits=len(invalid_conversion_tuples),
        error_messages=[str(error[1]) for error in invalid_conversion_tuples],
        dry_run=dry_run,
    )

    return run_result

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    return ProtocolSchema(
        "CM360OCI",
        [
            ("profile_id",
             str,
             Field(description="A Campaign Manager Profile ID .")
            ),
            ("encryptionEntityType",
             str,
             Field(description="The encryption entity type. One of DCM_ACCOUNT, DCM_ADVERTISER, DBM_PARTNER, DBM_ADVERTISER, ADWORDS_CUSTOMER	 or DFP_NETWORK_CODE")
            ),
            ("encryptionEntityId",
             str,
             Field(description="The encryption entity ID.")
            ),
            ("encryptionSource",
             str,
             Field(description="Describes whether the encrypted cookie was received from AD_SERVING or DATA_TRANSFER.")
            ),
            ("access_token",
             str,
             Field(description="A Campaign Manager 360 access token.")
            ),
            ("refresh_token",
             str,
             Field(description="A Campaign Manager 360 API refresh token.")
            ),
            ("client_id",
             str,
             Field(description="An OAuth2.0 Web Client ID.")
            ),
            ("client_secret",
             str,
             Field(description="An OAuth2.0 Web Client Secret.")
            ),
        ]
    )

  def fields(self) -> Sequence[str]:
    return CM_CONVERSION_FIELDS

  def batch_size(self) -> int:
    return 1000

  def validate(self) -> ValidationResult:
    """Validates the provided config.

    Returns:
      A ValidationResult for the provided config.
    """
    missing_encryption_fields = []
    error_msg = ""

    for encryption_field in self.encryption_info:
      if not self.encryption_info[encryption_field]:
        missing_encryption_fields.append(encryption_field)

    if missing_encryption_fields:
      error_msg = (
          "Config requires the following fields to be set: "
          f"{', '.join(missing_encryption_fields)}")
      return ValidationResult(False, [error_msg])

    return ValidationResult(True, [error_msg])

  def _validate_conversion(self, conversion: Mapping[str, Any]) -> Tuple[bool, Optional[str]]:
    """Validates a conversion.

    Arguments:
      conversion: The target conversion to be validated

    Returns:
      A tuple containing a boolean indicating whether or not the entry is valid 
      and an optional error message.
    """

    # checks for required fields
    for required_field in CM_REQUIRED_CONVERSIONS_FIELDS:
      if not conversion[required_field]:
        error_message = f"Missing required conversion field: {required_field}"
        return (False, error_message)

    # checks for mutually exclusive ids
    found_ids = []
    for id_field in CM_MUTUALLY_EXCLUSIVE_CONVERSIONS_FIELDS:
      if conversion.get(id_field):
        found_ids.append(id_field)

    ids_count = len(found_ids)
    if ids_count == 0:
      error_message = "No valid identifier was found."
      return (False, error_message)
    elif ids_count > 1:
      error_message = f"More than one mutually-exclusive identifier found: {found_ids}"
      return (False, error_message)

    return (True, "")
