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

import datetime
import enum
import json
import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from googleapiclient import discovery
import google_auth_httplib2
import google.oauth2.credentials

import errors
import immutabledict
import requests
from pydantic import Field
from utils import ProtocolSchema, RunResult, SchemaUtils, ValidationResult

CM_CONVERSION_FIELDS = [
  "floodlightConfigurationId",
  "floodlightActivityId",
  "encryptedUserId",
  "mobileDeviceId",
  "timestamp_micros",
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
]

CM_REQUIRED_CONVERSIONS_FIELDS = [
  "floodlightConfigurationId",
  "floodlightActivityId",
  "timestamp_micros",
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
  "token_uri",
  "client_id",
  "client_secret",
]

class Destination:
  """Implements DestinationProto protocol for Campaign Manager Offline Conversion Import."""

  def __init__(self, config: Dict[str, Any]):
    self.config = config  # Keeping a reference for convenience.
    self.profileId = config.get("profile_id")
    self.credentials = {}

    for credential in CM_CREDENTIALS:
      self.credentials[credential] = config.get(credential,"")
    
    self.encryption_info = {}
    self.encryption_info["encryptionEntityType"] = config.get("encryptionEntityType","")
    self.encryption_info["encryptionEntityId"] = config.get("encryptionEntityId","")
    self.encryption_info["encryptionEntitySource"] = config.get("encryptionEntitySource","")
    self.encryption_info["kind"] = config.get("kind","")
    self.validate()
    self._validate_credentials()
    
    # Authenticate using the supplied user account credentials
    self.http = self.authenticate_using_user_account()

  def authenticate_using_user_account(self):
    """Authorizes an httplib2.Http instance using user account credentials."""
    
    credentials = google.oauth2.credentials.Credentials(
    self.credentials['access_token'],
    refresh_token = self.credentials['refresh_token'],
    token_uri = self.credentials['token_uri'],
    client_id = self.credentials['client_id'],
    client_secret = self.credentials['client_secret'])

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
    t = conversion.get("timestamp_micros")
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
    service = discovery.build('dfareporting', 'v4', http=self.http)

    try:
      request = service.conversions().batchinsert(profileId=self.profileId,
                                            body=payload)

      response = request.execute()
      # Success is to be considered between 200 and 299:
      # https://developer.mozilla.org/en-US/docs/Web/HTTP/Status
      if response.status_code < 200 or response.status_code >= 300:
        raise errors.DataOutConnectorSendUnsuccessfulError(
            msg="Sending payload to CM360 did not complete successfully.",
            error_num=errors.ErrorNameIDMap.RETRIABLE_CM360_HOOK_ERROR_HTTP_ERROR,
        )
    except requests.ConnectionError:
      raise errors.DataOutConnectorSendUnsuccessfulError(
          msg="Sending payload to CM360 did not complete successfully.",
          error_num=errors.ErrorNameIDMap.RETRIABLE_CM360_HOOK_ERROR_HTTP_ERROR,
      )

  def send_data(self, input_data: List[Dict[str, Any]], dry_run:bool):
    """Builds payload and sends data to CM360 API."""

    valid_conversions = []
    invalid_conversions = []
    encryption_info = {}

    for i,entry in enumerate(input_data):
      conversion = {}
      for conversion_field in CM_CONVERSION_FIELDS:
        if conversion_field == "timestamp_micros":
          timestamp_micros = self._parse_timestamp_micros(entry)
          if timestamp_micros:
            conversion[conversion_field] = timestamp_micros
          else:
            conversion[conversion_field] = ""
        elif conversion_field == "conversion_kind":
          conversion["kind"] = str(entry.get(conversion_field, ""))
        else:
          conversion[conversion_field] = str(entry.get(conversion_field, ""))
      
      if self.validate_conversion(conversion):
        valid_conversions.append(conversion)
      else:
        invalid_conversions.append((i, errors.ErrorNameIDMap.CM_HOOK_ERROR_INVALID_CONVERSION_EVENT))

    if valid_conversions:
      payload = {}
      payload["encryptionInfo"] = self.encryption_info
      payload["conversions"] = valid_conversions

      if not dry_run:
        try:
          self._send_payload(payload)
        except (
              errors.DataOutConnectorSendUnsuccessfulError,
          ) as error:
            send_error = error.error_num
      else:
        print(
          "Dry-Run: CM conversions event will not be sent."
        )

    return RunResult(
      successful_hits=len(valid_conversions),
      failed_hits=len(invalid_conversions),
      error_messages=[str(error[1]) for error in invalid_conversions],
      dry_run=dry_run,
    )

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    return ProtocolSchema(
      "CM360OCI",
      [
        ("profile_id", str, Field(description="A Camaign Manager Profile ID .")),
        ("encryptionEntityType", str, Field(description="The encryption entity type.")),
        ("encryptionEntityId", str, Field(description="The encryption entity ID.")),
        ("encryptionSource", str, Field(description="Describes whether the encrypted cookie was received from ad serving or from Data Transfer.")),
        ("kind", str, Field(description="Identifies what kind of resource this is.")),
        ("access_token", str, Field(description="A Campaign Manager 360 access token.")),
        ("refresh_token", str, Field(description="A Campaign Manager 360 API refresh token.")),
        ("token_uri", str, Field(description="A Campaign Manager 360 API token uri.")),
        ("client_id", str, Field(description="An OAuth2.0 Web Client ID.")),
        ("client_secret", str, Field(description="An OAuth2.0 Web Client Secret.")),
      ]
    )

  def fields(self) -> Sequence[str]:
    return CM_CONVERSION_FIELDS

  def batch_size(self) -> int:
    return 10000

  def validate(self) -> ValidationResult:
    """Validates the provided config.

    Returns:
      A ValidationResult for the provided config.
    """
    missing_encryption_fields = []
    error_msg = ''

    for encryption_field in self.encryption_info:
      if not self.encryption_info[encryption_field]:
        missing_encryption_fields.append(encryption_field)

    if missing_encryption_fields:
      error_msg = (
        "Config requires the following fields to be set: "
        f"{', '.join(missing_encryption_fields)}")
      return ValidationResult(False, [error_msg])
      
    return ValidationResult(True, [error_msg])

  def validate_conversion(self, conversion) -> bool:
    """Validates the conversions list.

    Returns:
      A ValidationResult for the provided conversions list.
    """
    
    for required_field in CM_REQUIRED_CONVERSIONS_FIELDS:
      if not conversion[required_field]:
        return False

    invalid = True
    for id_field in CM_MUTUALLY_EXCLUSIVE_CONVERSIONS_FIELDS:
      if conversion[id_field]:
        invalid = False
        break

    if invalid:
      return False

    return True
