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
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import googleapiclient
from googleapiclient import discovery

import google_auth_httplib2
import google.oauth2.credentials

from utils import errors
from pydantic import Field
from utils.google_ads_utils import GoogleAdsUtils
from utils.protocol_schema import ProtocolSchema
from utils.run_result import RunResult
from utils.validation_result import ValidationResult

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
    "client_id",
    "client_secret",
]

API_VERSION = "v3"
SERVICE_URL = f"https://displayvideo.googleapis.com/$discovery/rest?version={API_VERSION}"

MEMBERSHIP_DURATION_DEFAULT = 540


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
    self.app_id = config.get("app_id")
    self.membership_duration_days = config.get(
        "membership_duration_days",
        MEMBERSHIP_DURATION_DEFAULT)

    self.credentials = {}
    for credential in DV_CREDENTIALS:
      self.credentials[credential] = config.get(credential, "")
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

  def _validate_entry(self, entry: Mapping[str, Any]) -> Tuple[bool, Optional[str]]:
    """Validates an audience entry.

    Arguments:
      entry: The target entry to be validated

    Returns:
      A tuple containing a boolean indicating whether or not the entry is valid
      and an optional error message.
    """
    if self.payload_type == PayloadTypes.CONTACT_INFO:
      non_empty_field = lambda field: field in entry and entry.get(field)
      has_email = any(
          non_empty_field(field) for field in ["email", "hashed_email"]
      )
      has_phone = any(
          non_empty_field(field) for field in ["phone_number", "hashed_phone_number"]
      )
      has_first_name = any(
          non_empty_field(field) for field in ["first_name", "hashed_first_name"]
      )
      has_last_name = any(
          non_empty_field(field) for field in ["last_name", "hashed_last_name"]
      )
      has_required_address_fields = all(
          non_empty_field(field) for field in ["zip_code", "country_code"]
      )
      has_complete_address = all(
          [has_first_name, has_last_name, has_required_address_fields]
      )
      if (has_first_name or
          has_last_name or
          has_required_address_fields
         ) and not has_complete_address:
        # Incomplete address
        return (False, "Incomplete address information.")
      elif any([has_email, has_phone, has_complete_address]):
        return (True, None)
      return (False, "ContactInfo payloads require at least one of: email, phone_number or address information.")
    else:
      if all(field in entry for field in DV_DEVICE_ID_FIELDS):
        return (True, None)
      else:
        return (False, f"MobileDeviceId payloads require {','.join(DV_DEVICE_ID_FIELDS)} fields.")

  def _get_audience_id(self) -> Optional[str]:
    """Get audience_id of an audience with the same name as provided, if it exists.

    Returns:
      The audience_id str, if the audience can be found.
      Returns None otherwise, which will cause a new audience to be created.
    """
    audiences = self.client.list(
        advertiserId=self.advertiser_id,
        filter=f"displayName:\"{self.audience_name}\""
    ).execute()
    for audience in audiences.get("firstAndThirdPartyAudiences", []):
      if audience["displayName"] == self.audience_name:
        # if and audience with the exact display name exists,
        # return the audience id
        audience_id = audience["name"].split("/")[1]
        return audience_id
  # if the audience is not found, return None and a new audience
  # with this display name will be created downstream.
    return None

  def _build_request_body(self, entries: List[Dict[str, Any]], is_create: bool) -> Dict[str, Any]:
    """Creates DV360 API request body."""
    body = {}
    inner_id_field = "contactInfos" if self.payload_type == PayloadTypes.CONTACT_INFO else "mobileDeviceIds"
    ids = {inner_id_field: [self._build_ids_object(entry) for entry in entries]}
    if is_create:
      # "create" exclusive fields
      outer_id_field = "contactInfoList" if self.payload_type == PayloadTypes.CONTACT_INFO else "mobileDeviceIdList"
      body["membershipDurationDays"] = self.membership_duration_days
      body["displayName"] = self.audience_name
      body["audienceType"] = "CUSTOMER_MATCH_CONTACT_INFO" if self.payload_type == PayloadTypes.CONTACT_INFO else "CUSTOMER_MATCH_DEVICE_ID"
      body["description"] = "Audience uploaded using Tightlock."
      if self.payload_type == PayloadTypes.DEVICE_ID:
        body["appId"] = self.app_id
    else:
      # "update" exclusive fields
      outer_id_field = "addedContactInfoList" if self.payload_type == PayloadTypes.CONTACT_INFO else "addedMobileDeviceIdList"
      body["advertiserId"] = self.advertiser_id

    # add ids to body with the appropriate field depending on the operation
    body[outer_id_field] = ids
    return body

  def _build_ids_object(self,
                        entry: Dict[str, Any]) -> Dict[str, Any] | str | None:
    """Build an ids object compatible with the payload type using the provided entry.

    Args:
      entry: One row of the input_data read from the source.

    Returns:
      A formatted id object compatible with the payload type.

    """
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

  def _send_payload(self, valid_entries: List[Dict[str, Any]], audience_id: Optional[str]) -> None:
    """Sends Customer Match payload to DV360 API.

    Args:
      valid_entries: Validated customer match list entries.
      audience_id: Id of existing list to update. Will create a new list if it is None.

    Returns:
      results: Includes request body, status_code, error_msg, response body and
      debug flag.
    """
    is_create = audience_id is None
    body = self._build_request_body(
        entries=valid_entries,
        is_create=is_create
    )
    try:
      if is_create:
        # creates new audience
        request = self.client.create(
            advertiserId=self.advertiser_id,
            body=body
        )
      else:
        # adds to existent audience
        request = self.client.editCustomerMatchMembers(
            firstAndThirdPartyAudienceId=audience_id,
            body=body
        )

      response = request.execute()
      if response:
        action_verb = "created" if is_create else "updated"
        print(f"{self.audience_name} customer match list {action_verb} successfully with {len(valid_entries)} entries.")
    except googleapiclient.errors.HttpError as http_error:
      if http_error.resp.status >= 400 and http_error.resp.status < 500:
        error_num = errors.ErrorNameIDMap.NON_RETRIABLE_ERROR_EVENT_NOT_SENT
      else:
        error_num = errors.ErrorNameIDMap.RETRIABLE_DV360_HOOK_ERROR_HTTP_ERROR
      raise errors.DataOutConnectorSendUnsuccessfulError(
          msg=f"Sending payload to DV360 did not complete successfully: {http_error}",
          error_num=error_num,
      )

  def send_data(self, input_data: List[Mapping[str, Any]], dry_run: bool) -> Optional[RunResult]:
    """Builds payload and sends data to DV360 API."""

    valid_entry_tuples = []
    invalid_entry_tuples = []
    http_error = None

    for i, entry in enumerate(input_data):
      is_valid, error_message = self._validate_entry(entry)
      if is_valid:
        valid_entry_tuples.append((i, entry))
      else:
        invalid_entry_tuples.append((i, error_message))

    if valid_entry_tuples:
      if not dry_run:
        try:
          valid_entries = [entry[1] for entry in valid_entry_tuples]
          self._send_payload(valid_entries, self._get_audience_id())
        except (
            errors.DataOutConnectorSendUnsuccessfulError,
        ) as error:
          http_error = error.msg
      else:
        print(
            "Dry-Run: DV Customer Match audiences will not be sent."
        )

    if http_error:
      invalid_entry_tuples += [(entry_tuple[0], http_error) for entry_tuple in valid_entry_tuples]
      valid_entry_tuples = []

    print(f"Sent entries: {len(valid_entry_tuples)}")
    print(f"Invalid entries: {len(invalid_entry_tuples)}")

    for invalid_entry in invalid_entry_tuples:
      entry_index = invalid_entry[0]
      error = invalid_entry[1]
      # TODO(b/272258038): TBD What to do with invalid events data.
      print(f"entry_index: {entry_index}; error: {error}")

    run_result = RunResult(
        successful_hits=len(valid_entry_tuples),
        failed_hits=len(invalid_entry_tuples),
        error_messages=[str(error[1]) for error in invalid_entry_tuples],
        dry_run=dry_run,
    )

    return run_result

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    return ProtocolSchema(
        "DV360CM",
        [
            ("advertiser_id",
             str,
             Field(description="A DV360 Advertiser ID.")
            ),
            ("audience_name",
             str,
             Field(description="The display name of the audience list.")
            ),
            ("payload_type",
             PayloadTypes,
             Field(description="DV360 Customer Match Payload Type")
            ),
            ("app_id",
             str,
             Field(
                 condition_field="payload_type",
                 condition_target="mobile_device_id",
                 description="An app Id that matches with the type of the mobileDeviceIds being uploaded. Required for 'Mobile Device Id' payload type.")
            ),
            ("membership_duration_days",
             int,
             Field(
                 default=MEMBERSHIP_DURATION_DEFAULT,
                 description="The duration in days that an entry remains in the audience after the qualifying event. If the audience has no expiration, set the value of this field to 10000. Otherwise, the set value must be greater than 0 and less than or equal to 540.")
            ),
            ("access_token",
             str,
             Field(description="An OAuth2.0 access token.")
            ),
            ("refresh_token",
             str,
             Field(description="An Oauth2.0 refresh token.")
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
    if self.payload_type == PayloadTypes.CONTACT_INFO:
      return DV_CONTACT_INFO_FIELDS
    elif self.payload_type == PayloadTypes.DEVICE_ID:
      return DV_DEVICE_ID_FIELDS
    else:
      raise NameError(f"Unknown payload type: {self.payload_type}")

  def batch_size(self) -> int:
    return 50000

  def validate(self) -> ValidationResult:
    """Validates the provided config.

    Returns:
      A ValidationResult for the provided config.
    """
    try:
      PayloadTypes(self.payload_type)
    except ValueError:
      return ValidationResult(False, [f"Invalid Payload Type: {self.payload_type}"])

    if self.payload_type == PayloadTypes.DEVICE_ID:
      if not self.app_id:
        return ValidationResult(False, ["App Id is required for 'Mobile Device Id' payload type."])

    if not all([self.advertiser_id, self.audience_name]):
      return ValidationResult(False, ["Required config values 'advertiser_id' or 'audience_name' are missing or empty."])

    if not all([credential in self.config for credential in DV_CREDENTIALS]):
      return ValidationResult(False, [f"One of the following credentials missing or empty: {','.join(DV_CREDENTIALS)}"])

    return ValidationResult(True, [])
