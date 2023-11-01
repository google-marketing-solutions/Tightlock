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

"""DV360 Customer Match Upload destination implementation."""

# pylint: disable=raise-missing-from

import enum
from typing import Any, Dict, List, Mapping, Optional, Sequence

from googleapiclient import discovery
import google_auth_httplib2
import google.oauth2.credentials

import errors
import requests
from pydantic import Field
from utils import ProtocolSchema, RunResult, ValidationResult

DV_CONTACT_INFO_FIELDS = [
  "email",
  "hashed_email",
  "phone_number",
  "hashed_phone_number",
  "first_name",
  "hashed_first_name",
  "last_name",
  "hashed_last_name",
  "zip_code",
  "country_code",
]

DV_DEVICE_ID_FIELDS = [
  "mobile_device_id",
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

DV_CREDENTIALS = [
  "access_token",
  "refresh_token",
  "token_uri",
  "client_id",
  "client_secret",
]

class PayloadTypes(str, enum.Enum):
  """DV360 supported payload types."""

  CONTACT_INFO = "contact_info"
  DEVICE_ID = "mobile_device_id"

class Destination:
  """Implements DestinationProto protocol for DV360 Customer Match Audiences Upload."""

  def __init__(self, config: Dict[str, Any]):
    self.config = config  # Keeping a reference for convenience.
    self.advertiser_id = config.get("advertiser_id")
    self.payload_type = config.get("payload_type")
    self.audience_name = config.get("audience_name")
    self.credentials = {}

    for credential in DV_CREDENTIALS:
      self.credentials[credential] = config.get(credential,"")
    
    # self.encryption_info = {}
    # self.encryption_info["encryptionEntityType"] = config.get("encryptionEntityType","")
    # self.encryption_info["encryptionEntityId"] = config.get("encryptionEntityId","")
    # self.encryption_info["encryptionEntitySource"] = config.get("encryptionEntitySource","")
    # self.encryption_info["kind"] = config.get("kind","")
    # self.validate()
    self._validate_credentials()
    
    # Authenticate using the supplied user account credentials
    self.http = self.authenticate_using_user_account()
    self.client = discovery.build('displayvideo', 'v3', discoveryServiceUrl="https://displayvideo.googleapis.com/$discovery/rest?version=v3", http=self.http).firstAndThirdPartyAudiences()

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


  def _validate_entry(self, entry: Mapping[str, Any]) -> bool:
    """Validates an audience entry.
    
    Arguments:
      entry: The target entry to be validated

    Returns:
      A boolean indicating whether or not the entry is valid.

    """
    if self.payload_type == PayloadTypes.CONTACT_INFO:
      has_email = any(field in entry for field in ["email", "hashed_email"])
      has_phone = any(field in entry for field in ["phone_number", "hashed_phone_number"])
      has_first_name = any(field in entry for field in ["first_name", "hashed_first_name"])
      has_last_name = any(field in entry for field in ["last_name", "hashed_last_name"])
      has_required_address_fields = all(field in entry for field in ["zip_code", "country_code"])
      has_complete_address = all([has_first_name, has_last_name, has_required_address_fields])
      return any([has_email, has_phone, has_complete_address])
    else:
      return all(field in entry for field in DV_DEVICE_ID_FIELDS)

  def _is_new_list(self) -> bool:
    audiences = self.client.list(advertiserId=self.advertiser_id, filter=f"displayName:{self.audience_name}").execute()
    print(f"Audiences: {audiences}")
    


  def _validate_credentials(self) -> None:
    """Validate credentials.

    Raises:
      Exception: If credential combination does not meet criteria.
    """
    for credential in DV_CREDENTIALS:
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
    """Sends Customer Match payload to DV360 API.

    Args:
      payload: Parameters containing required data for conversion tracking.

    Returns:
      results: Includes request body, status_code, error_msg, response body and
      debug flag.
    """

    # Construct a service object via the discovery service.
    service = discovery.build('displayvideo', 'v3', http=self.http)

    try:
      request = service.firstAndThirdPartyAudiences().create(advertiserId=self.advertiser_id,
                                                             body=payload)

      response = request.execute()
      # Success is to be considered between 200 and 299:
      # https://developer.mozilla.org/en-US/docs/Web/HTTP/Status
      if response.status_code < 200 or response.status_code >= 300:
        raise errors.DataOutConnectorSendUnsuccessfulError(
            msg="Sending payload to DV360 did not complete successfully.",
            error_num=errors.ErrorNameIDMap.RETRIABLE_DV360_HOOK_ERROR_HTTP_ERROR,
        )
    except requests.ConnectionError:
      raise errors.DataOutConnectorSendUnsuccessfulError(
          msg="Sending payload to DV360 did not complete successfully.",
          error_num=errors.ErrorNameIDMap.RETRIABLE_DV360_HOOK_ERROR_HTTP_ERROR,
      )

  def send_data(self, input_data: List[Mapping[str, Any]], dry_run:bool) -> Optional[RunResult]:
    """Builds payload and sends data to DV360 API."""

    valid_entries = []
    invalid_entries = []

    # validate conversions (have at least one of the valid ids)
    # call list and filter by display name
    # filter again to ensure has the exact same display name
    # depending on result, create or edit

    for entry in input_data:
      if self._validate_entry(entry):
        valid_entries.append(entry)
      else:
        invalid_entries.append(entry)

    if valid_entries:

      if not dry_run:
        try:
          self._send_payload(valid_entries, self._is_new_list())
        except (
              errors.DataOutConnectorSendUnsuccessfulError,
          ) as error:
            send_error = error.error_num
      else:
        print(
          "Dry-Run: DV Customer Match audiences will not be sent."
        )

    run_result = RunResult(
      successful_hits=len(valid_conversions),
      failed_hits=len(invalid_conversions),
      error_messages=[str(error[1]) for error in invalid_conversions],
      dry_run=dry_run,
    )

    return run_result

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    return ProtocolSchema(
      "DV360CM",
      [
        ("advertiser_id", str, Field(description="A DV360 Advertiser ID.")),
        ("audience_name", str, Field(description="The display name of the audience list.")),
        # ("description", str, Field(description="The description of the audience list.")),
        ("payload_type", PayloadTypes, Field(description="DV360 Customer Match Payload Type")),
        # ("encryptionEntityType", str, Field(description="The encryption entity type.")),
        # ("encryptionEntityId", str, Field(description="The encryption entity ID.")),
        # ("encryptionSource", str, Field(description="Describes whether the encrypted cookie was received from ad serving or from Data Transfer.")),
        # ("kind", str, Field(description="Identifies what kind of resource this is.")),
        ("access_token", str, Field(description="A Campaign Manager 360 access token.")),
        ("refresh_token", str, Field(description="A Campaign Manager 360 API refresh token.")),
        ("token_uri", str, Field(description="A Campaign Manager 360 API token uri.")),
        ("client_id", str, Field(description="An OAuth2.0 Web Client ID.")),
        ("client_secret", str, Field(description="An OAuth2.0 Web Client Secret.")),
      ]
    )

  def fields(self) -> Sequence[str]:
    # TODO(caiotomazelli): Review fields
    if self.payload_type == PayloadTypes.CONTACT_INFO:
      return DV_CONTACT_INFO_FIELDS
    elif self.payload_type == PayloadTypes.DEVICE_ID:
      return DV_DEVICE_ID_FIELDS
    else:
      raise NameError(f"Unknown payload type: {self.payload_type}")

  def batch_size(self) -> int:
    # TODO(caiotomazelli): Review batch size
    return 50000

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
    """Validates a conversion.

    Returns:
      True if the conversion is valid, False if it isn't.
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
