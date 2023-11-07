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
from re import A
from typing import Any, Dict, List, Mapping, Optional, Sequence

from googleapiclient import discovery
import google_auth_httplib2
import google.oauth2.credentials
import google.auth.transport.requests

import errors
import requests
from pydantic import Field
from utils import GoogleAdsUtils, ProtocolSchema, RunResult, ValidationResult

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

DV_CREDENTIALS = [
    "access_token",
    "refresh_token",
    "token_uri",
    "client_id",
    "client_secret",
]

API_VERSION = "v3"
SERVICE_URL = f"https://displayvideo.googleapis.com/$discovery/rest?version={API_VERSION}"

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
      self.credentials[credential] = config.get(credential, "")

    # self.validate()
    self._validate_credentials()

    # Authenticate using the supplied user account credentials
    self.http = self.authenticate_using_user_account()
    self.client = discovery.build(
        "displayvideo",
        API_VERSION,
        discoveryServiceUrl=SERVICE_URL,
        http=self.http
    ).firstAndThirdPartyAudiences()

  def authenticate_using_user_account(self):
    """Authorizes an httplib2.Http instance using user account credentials."""

    # fix refresh token not working
    credentials = google.oauth2.credentials.Credentials(
        self.credentials["access_token"],
        refresh_token=self.credentials["refresh_token"],
        token_uri=self.credentials["token_uri"],
        client_id=self.credentials["client_id"],
        client_secret=self.credentials["client_secret"]
    )

    # TODO(caiotomazelli): Fix credentials refreshing
    # Refresh credentials using the refresh_token
    # request = google.auth.transport.requests.Request()
    # credentials.refresh(request)
    
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
      non_empty_field = lambda field: field in entry and entry.get(field)
      has_email = any(non_empty_field(field) for field in ["email", "hashed_email"])
      has_phone = any(non_empty_field(field) for field in ["phone_number", "hashed_phone_number"])
      has_first_name = any(non_empty_field(field) for field in ["first_name", "hashed_first_name"])
      has_last_name = any(non_empty_field(field) for field in ["last_name", "hashed_last_name"])
      has_required_address_fields = all(non_empty_field(field) for field in ["zip_code", "country_code"])
      has_complete_address = all([has_first_name, has_last_name, has_required_address_fields])
      return any([has_email, has_phone, has_complete_address])
    else:
      return all(field in entry for field in DV_DEVICE_ID_FIELDS)

  def _get_audience_id(self) -> Optional[str]:
    audiences = self.client.list(advertiserId=self.advertiser_id, filter=f"displayName:\"{self.audience_name}\"").execute()
    for audience in audiences['firstAndThirdPartyAudiences']:
      if audience["displayName"] == self.audience_name:
        # if the audience already exists, return the audience id
        audience_id = audience["name"].split("/")[1]
        return audience_id
   # if the audience is not found, return None and a new audience with this display name will be created downstream. 
    return None

      
  def _build_request_body(self, entries: List[Dict[str, Any]], is_create: bool) -> Dict[str, Any]:
    """Creates DV360 API request body."""
    body = {}
    inner_field = "contactInfos" if self.payload_type == PayloadTypes.CONTACT_INFO else "mobileDeviceIds"
    ids = {inner_field: [self._build_ids_object(entry) for entry in entries]}
    if is_create:
      outer_field = "contactInfoList" if self.payload_type == PayloadTypes.CONTACT_INFO else "mobileDeviceIdList"
      body[outer_field] = ids
      # TODO(caiotomazelli): Expose membership duration field
      body["membershipDurationDays"] = 540
    else:
      outer_field = "addedContactInfoList" if self.payload_type == PayloadTypes.CONTACT_INFO else "addedMobileDeviceIdList"
      body["advertiserId"] = self.advertiser_id
      body[outer_field] = ids

    # TODO(caiotomazelli): Review description
    body["description"] = "Audience uploaded by Tightlock."
    return body
    
  def _build_ids_object(self, entry: Dict[str, Any]) -> Dict[str, Any] | str | None:
    """Build """
    ids = None
    if self.payload_type == PayloadTypes.CONTACT_INFO:
      ids = {}
      email = entry.get("email")
      phone_number = entry.get("phone_number")
      hashed_email = entry.get("hashed_email")
      hashed_phone_number = entry.get("hashed_phone_number")
      first_name = entry.get("first_name")
      last_name = entry.get("last_name")
      hashed_first_name = entry.get("hashed_first_name")
      hashed_last_name = entry.get("hashed_last_name")
      country_code = entry.get("country_code")
      zip_code = entry.get("zip_code")

      # Email
      if email:
        ids["hashedEmails"] = []
        ids["hashedEmails"].append(GoogleAdsUtils().normalize_and_hash_email_address(email))
      elif hashed_email:
        ids["hashedEmails"] = []
        ids["hashedEmails"].append(hashed_email)
      # Phone Number
      if phone_number:
        ids["hashedPhoneNumbers"] = []
        ids["hashedPhoneNumbers"].append(GoogleAdsUtils().normalize_and_hash(phone_number))
      elif hashed_phone_number:
        ids["hashedPhoneNumbers"] = []
        ids["hashedPhoneNumbers"].append(hashed_phone_number)
      # First Name
      if first_name:
        ids["hashedFirstName"] = GoogleAdsUtils().normalize_and_hash(first_name)
      elif hashed_first_name:
        ids["hashedFirstName"] = hashed_first_name
      # Last Name
      if last_name:
        ids["hashedLastName"] = GoogleAdsUtils().normalize_and_hash(last_name)
      elif hashed_last_name:
        ids["hashedLastName"] = hashed_last_name
      # Other Address fields
      if country_code:
        ids["countryCode"] = country_code
      if zip_code:
        ids["zipCodes"] = []
        ids["zipCodes"].append(zip_code)
    else:
      # Mobile Device Id
      ids = entry.get("mobile_device_id")

    return ids

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

  def _send_payload(self, valid_entries: List[Dict[str, Any]], audience_id: Optional[str]) -> None:
    """Sends Customer Match payload to DV360 API.

    Args:
      valid_entries: Validated customer match list entries.

    Returns:
      results: Includes request body, status_code, error_msg, response body and
      debug flag.
    """

    body = self._build_request_body(
        entries=valid_entries,
        is_create=(audience_id is None)
    )
    print(f"Body: {body}")
    try:
      if audience_id:
        # adds to existent audience
        request = self.client.editCustomerMatchMembers(
            firstAndThirdPartyAudienceId=audience_id,
            body=body
        )
      else:
        # creates new audience
        request = self.client.create(
            advertiserId=self.advertiser_id,
            body=body
        )

        response = request.execute()
        # TODO(caiotomazelli): improve logging

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
          self._send_payload(valid_entries, self._get_audience_id())
        except (
              errors.DataOutConnectorSendUnsuccessfulError,
          ) as error:
            send_error = error.error_num
      else:
        print(
          "Dry-Run: DV Customer Match audiences will not be sent."
        )

    run_result = RunResult(
      successful_hits=len(valid_entries),
      failed_hits=len(invalid_entries),
      error_messages=[str(error[1]) for error in invalid_entries],
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
    # TODO(caiotomazelli): Check if this is needed and implement or delete
    return True
