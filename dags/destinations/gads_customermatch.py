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

"""Google Ads Customer Match destination implementation."""
import json

from pydantic import Field
from google.ads.googleads.errors import GoogleAdsException
from typing import Any, Dict, List, Mapping, Optional, Sequence
from utils import GoogleAdsUtils, ProtocolSchema, RunResult, ValidationResult, TadauMixin, AdsPlatform, EventAction

# Default batch size of 1000 user identifiers
_BATCH_SIZE = 1000

_USER_IDENTIFIER_FIELDS = [
  "email",
  "phone",
  "first_name",
  "last_name",
  "country_code",
  "postal_code"
]

_OTHER_FIELDS = [
    "ad_user_data_consent",
    "ad_personalization_consent",
]


def _construct_error_message(api_error: Any) -> str:
  """Construct an error message from an API error object."""
  return (
    "A partial failure at index "
    f"{api_error.location.field_path_elements[0].index} occurred.\n"
    f"Error message: {api_error.message}\n"
    f"Error code: {api_error.error_code}"
  )


class _UserDataScrubber:
  """Util class for scrubbing user identifier data."""

  def __init__(self, debug: bool = False):
    self._debug = debug
    self._user_data = {}
    self._utils = GoogleAdsUtils()

  def scrub_user_data(self, user_data: Mapping[str, Any]) -> Mapping[str, Any]:
    """Scrubs the user data this scrubber was created with.

    Args:
      user_data: Dict of user data to scrub.

    Raises:
      ValueError: Raised if any of the required mailing address fields are
        missing.
    """
    # Trimming dict, removing any keys with empty/whitespace string values
    self._user_data = {k: v for k, v in user_data.items() if v and v.strip()}
    if self._debug:
      print(f"Scrubbing user data: {json.dumps(self._user_data, indent=4)}")

    self._scrub_email()
    self._scrub_phone_number()
    self._scrub_mailing_name_fields()

    if self._debug:
      print(f"Scrubbed user data: {json.dumps(self._user_data, indent=4)}")
    return self._user_data

  def _scrub_email(self) -> None:
    if self._user_data.get("email", None):
      self._user_data["email"] = self._utils.normalize_and_hash_email_address(
        email_address=self._user_data["email"]
      )

  def _scrub_phone_number(self) -> None:
    if self._user_data.get("phone", None):
      self._user_data["phone"] = self._utils.normalize_and_hash(
        self._user_data["phone"]
      )

  def _scrub_mailing_name_fields(self) -> None:
    self._check_for_all_postal_fields()

    if self._user_data.get("first_name", None):
      self._user_data["first_name"] = self._utils.normalize_and_hash(
        self._user_data["first_name"]
      )

    if self._user_data.get("last_name", None):
        self._user_data["last_name"] = self._utils.normalize_and_hash(
          self._user_data["last_name"]
      )

  def _check_for_all_postal_fields(self) -> None:
    required_keys = ("first_name", "last_name", "country_code", "postal_code")
    is_postal_keys_available_flags = []
    for required_key in required_keys:
      is_postal_keys_available_flags.append(required_key in self._user_data)

    if not any(is_postal_keys_available_flags):
      if self._debug:
        print("No postal fields, so skipping postal fields scrubbing steps")
      return

    if not all(is_postal_keys_available_flags):
      missing_keys = required_keys - self._user_data.keys()
      if self._debug:
        print(
          "Raising exception because the following required mailing "
          f"address keys are missing: {missing_keys}"
        )
      raise ValueError(f"Missing mailing address fields:  {missing_keys}")


class Destination(TadauMixin):
  """Implements DestinationProto protocol for Google Ads Customer Match.
  
  This destination assumes that the data source has 1 row per user, and 
  that each row can have multiple user identifiers set (ie email address and 
  phone number).  This destination will then generate 1-or-more user 
  identifiers to add to the user data upload job.

  Hence, not all fields specified by this destination are required,
  but at least one of them must be supplied.
  """

  def __init__(self, config: Dict[str, Any]):
    """ Initializes Google Ads OCI Destination Class.

    Args:
      config: Configuration object to hold environment variables
    """
    super().__init__()  # Instantiates TadauMixin
    self._config = config
    self._debug = config.get("debug", False)
    self._customer_id = config.get("login_customer_id")

    self._client = GoogleAdsUtils().build_google_ads_client(self._config)
    self._offline_user_data_job_service = self._client.get_service(
      "OfflineUserDataJobService"
    )
    self._ads_service = self._client.get_service(
      "GoogleAdsService"
    )

    # Request-level consent flags
    self.ad_user_data_consent = None
    self.ad_personalization_consent = None

  def send_data(
      self, input_data: List[Mapping[str, Any]], dry_run: bool
  ) -> Optional[RunResult]:
    """Builds payload and sends data to Google Ads API.

    Args:
      input_data: A list of rows to send to the API endpoint.
      dry_run: If True, will not send data to API endpoints.

    Returns: A RunResult summarizing success / failures, etc.
    """
    if len(input_data) == 0:
      print("No rows of user data to send, exiting out of destination.")
      return RunResult(dry_run=dry_run, successful_hits=0, failed_hits=0)

    user_data_operations = []
    failures = []

    print(f"Processing {len(input_data)} user records")
    for index, user_data in enumerate(input_data):
      try:
        user_operation = self._build_user_data_operation_from_row(user_data)
        user_data_operations.append(user_operation)
        self._update_consent_metadata(index, user_data)
      except ValueError as ve:
        err_msg = f"Could not process data at row '{index}':  {str(ve)}"
        print(err_msg)
        failures.append(err_msg)
    print(f"There were '{len(failures)}' user rows that couldn't be processed.")

    # No point in continuing if doing a dry run, since the above steps
    #   detail how many rows were processed, and how many operations
    #   were created
    if dry_run:
      print("Running as a dry run, so skipping upload steps.")
      return RunResult(
        dry_run=True,
        successful_hits=len(user_data_operations) - len(failures),
        failed_hits=len(failures),
        error_messages=failures
      )

    try:
      upload_job_id = self._create_user_upload_job()
      add_user_data_request = self._create_add_user_data_request(
          upload_job_id, user_data_operations
      )

      response = self._offline_user_data_job_service.add_offline_user_data_job_operations(
          request=add_user_data_request
      )
     
      response_failures = self._process_response_for_failures(response)
      failures.append(response_failures)
      
      self._offline_user_data_job_service.run_offline_user_data_job(
          resource_name=upload_job_id
      )
    except GoogleAdsException as e:
      failures = [e for _ in user_data_operations]

    run_result = RunResult(
        successful_hits=len(user_data_operations) - len(failures),
        failed_hits=len(failures),
        error_messages=failures
    )

    # Collect usage data
    self.send_usage_event(
        ads_platform=AdsPlatform.GADS_CUSTOMER_MATCH,
        event_action=EventAction.AUDIENCE_UPDATED,
        run_result=run_result,
        ads_resource="UserListId",
        ads_resource_id=self._config["user_list_id"]
    )

    return run_result

  def _update_consent_metadata(self, index: int, user_data: Mapping[str, Any]):
    """Updates the consent metadata with a new user_data row.
    
    Consent metadata needs to be consensual for the whole batch and will default
    to None if divergence occurs.

    Args:
      index: The index of the user_data row.
      user_data:  A row of user data from the source.
    """
    ad_user_data = user_data.get("ad_user_data_consent")
    ad_personalization = user_data.get("ad_personalization_consent")

    for global_consent_var_name, row_consent_var in [
        ("ad_user_data_consent", ad_user_data),
        ("ad_personalization_consent", ad_personalization)
    ]:
      global_consent_var = getattr(self, global_consent_var_name)
      if global_consent_var and row_consent_var:
        updated_consent = (
            global_consent_var
            if global_consent_var == row_consent_var
            else None
        )
        setattr(self, global_consent_var_name, updated_consent)
      # allow global None to be update on the first row of the batch
      elif index == 0 and row_consent_var:
        setattr(self, global_consent_var_name, row_consent_var)

  def _build_user_data_operation_from_row(
      self, user_data: Mapping[str, Any]
  ) -> Any:
    """Builds a Google Ads OfflineUserDataJobOperation object from user data.
    
    Args:
      user_data:  A row of user data from the Source.
    
    Returns:
      A Google Ads OfflineUserDataJobOperation object populated with 1-or-more
      identifiers.
    """
    scrubber = _UserDataScrubber(self._debug)
    scrubbed_user_data = scrubber.scrub_user_data(user_data)

    user_data_payload = self._create_user_data_payload(scrubbed_user_data)

    operation = self._client.get_type("OfflineUserDataJobOperation")
    operation.create = user_data_payload
    return operation

  def _create_user_data_payload(self, user_data: Mapping[str, Any]) -> Any:
    """Populates the provided Google Ads UserData payload object.

    Args:
      user_data:  Row of user data with various identifiers.

    Returns:
      UserData Google Ads API payload object.

    Raises:
      ValueError:  Raised if any of the required mailing address fields are
        not present.
    """
    payload = self._client.get_type("UserData")

    if "email" in user_data:
      user_identifier = self._client.get_type("UserIdentifier")
      user_identifier.hashed_email = user_data["email"]
      payload.user_identifiers.append(user_identifier)

    if "phone" in user_data:
      user_identifier = self._client.get_type("UserIdentifier")
      user_identifier.hashed_phone_number = user_data["phone"]
      payload.user_identifiers.append(user_identifier)

    if "first_name" in user_data:
      user_identifier = self._client.get_type("UserIdentifier")
      address_info = user_identifier.address_info

      address_info.hashed_first_name = user_data["first_name"]
      address_info.hashed_last_name = user_data["last_name"]
      address_info.country_code = user_data["country_code"]
      address_info.postal_code = user_data["postal_code"]

      payload.user_identifiers.append(user_identifier)
  
    return payload

  def _create_user_upload_job(self) -> str:
    """Sends a request to create a user data upload job.

    Returns:
      The resource name ID (ie, 'jobs/some_id') of the created job
    """
    user_list_resource_name = self._ads_service.user_list_path(
      self._config["login_customer_id"],
      self._config["user_list_id"]
    )
    offline_user_data_job = self._client.get_type("OfflineUserDataJob")
    offline_user_data_job.type_ = (
      self._client.enums.OfflineUserDataJobTypeEnum.CUSTOMER_MATCH_USER_LIST)
    offline_user_data_job.customer_match_user_list_metadata.user_list = (
      user_list_resource_name
    )
    if self.ad_user_data_consent:
      offline_user_data_job.customer_match_user_list_metadata.consent.ad_user_data = (
          self.ad_user_data_consent)

    if self.ad_personalization_consent:
      offline_user_data_job.customer_match_user_list_metadata.consent.ad_personalization = (
          self.ad_personalization_consent)


    create_offline_user_data_job_response = (
      self._offline_user_data_job_service.create_offline_user_data_job(
        customer_id=self._customer_id, job=offline_user_data_job)
    )

    offline_user_data_job_resource_name = (
      create_offline_user_data_job_response.resource_name
    )
    print(
      "Created an offline user data job with resource name: "
      f"'{offline_user_data_job_resource_name}'."
    )

    return offline_user_data_job_resource_name

  def _create_add_user_data_request(
      self, upload_job_id: str, user_operations: List[Any]) -> Any:
    # Create and send a request to add the operations to the job
    request = self._client.get_type("AddOfflineUserDataJobOperationsRequest")
    request.resource_name = upload_job_id
    request.operations = user_operations
    request.enable_partial_failure = True

    return request

  def _process_response_for_failures(self, response: Any) -> Sequence[str]:
    """Prints and returns partial error details.
    
    Args:
      response:  The Google Ads AddOfflineUserDataJobOperationsResponse
        returned by the API call to add user operations to the job.

    Returns:
      Sequence of error details, one message per failed user data operation.
    """
    failures = []
    partial_failure_payload = getattr(response, "partial_failure_error", None)

    if getattr(partial_failure_payload, "code", None) != 0:
      error_details_payload = getattr(partial_failure_payload, "details", [])
      for error_detail in error_details_payload:
        failure_message_type = self._client.get_type("GoogleAdsFailure")
        failure_payload = type(failure_message_type).deserialize(
          error_detail.value
        )

        for error in failure_payload.errors:
          error_message = _construct_error_message(error)
          print(error_message)
          failures.append(error_message)

    return failures

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    """Returns the required metadata for this destination config.

    Returns:
      An optional ProtocolSchema object that defines the
      required and optional metadata used by the implementation
      of this protocol.
    """
    return ProtocolSchema(
      "GADS_CUSTOMERMATCH",
      [
        # API config variables
        ("client_id",
         str,
         Field(description="An OAuth2.0 Web Client ID.")),
        ("client_secret",
         str,
         Field(description="An OAuth2.0 Web Client Secret.")),
        ("developer_token",
         str,
         Field(description="A Google Ads Developer Token.")),
        ("login_customer_id",
         str,
         Field(
           description="A Google Ads Login Customer ID (without hyphens).")),
        ("refresh_token",
         str,
         Field(description="A Google Ads API refresh token.")),

        # User Data List variables
        ("user_list_id",
         str,
         Field(
           description="ID of the user list this destination will add users "
                       "to.")),
      ]
    )

  def fields(self) -> Sequence[str]:
    """Lists required fields for the destination input data.

    Returns:
      A sequence of fields.
    """
    return _USER_IDENTIFIER_FIELDS + _OTHER_FIELDS

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
    return GoogleAdsUtils().validate_google_ads_config(self._config)
