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


from pydantic import Field
from typing import Any, Dict, List, Mapping, Optional, Sequence
from utils import GoogleAdsUtils, ProtocolSchema, RunResult, ValidationResult

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

  def __init__(self):
    self._user_data = {}
    self._utils = GoogleAdsUtils()

  def scrub_user_data(self, user_data: Mapping[str, Any]):
    self._user_data = user_data

    self._scrub_email()
    self._scrub_phone_number()
    self._scrub_mailing_name_fields()

  def _scrub_email(self) -> None:
    if "email" in self._user_data:
      self._user_data["email"] = self._utils.normalize_and_hash_email_address(
        email_address=self._user_data["email"]
      )

  def _scrub_phone_number(self) -> None:
    if "phone" in self._user_data:
      self._user_data["phone"] = self._utils.normalize_and_hash(
        self._user_data["phone"]
      )

  def _scrub_mailing_name_fields(self) -> None:
    if "first_name" in self._user_data:
      self._user_data["first_name"] = self._utils.normalize_and_hash(
        self._user_data["first_name"]
      )

    if "last_name" in self._user_data:
      self._user_data["first_name"] = self._utils.normalize_and_hash(
        self._user_data["last_name"]
      )


class Destination:
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
    for user_data in input_data:
      try:
        user_operation = self._build_user_data_operation_from_row(user_data)
        user_data_operations.append(user_operation)
      except ValueError as ve:
        failures.append(str(ve))
    print(
      f"There were '{len(failures)}' user rows that couldn't be processed.")

    # No point in continuing if doing a dry run, since the above steps
    #   detail how many rows were processed, and how many operations
    #   were created
    if dry_run:
      print("Running as a dry run, so skipping upload steps.")
      return RunResult(dry_run=dry_run, successful_hits=0, failed_hits=0)

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

    return RunResult(
      successful_hits=len(user_data_operations) - len(failures),
      failed_hits=len(failures),
      error_messages=failures
    )

  def _build_user_data_operation_from_row(
      self, user_data: Mapping[str, Any]
  ) -> Any:
    """Builds a Google Ads OfflineUserDataJobOperation object from user data.
    
    Args:
      user_data:  A row of user data from the Source.
    
    Returns:
      A Google Ads OfflineUserDataJobOperation object populated with 1-or-more
      identifiers.

    Raises:
      ValueError:  Raised if any of the required mailing address fields are
        missing.
    """
    scrubber = _UserDataScrubber()
    scrubber.scrub_user_data(user_data)

    user_data_payload = self._create_user_data_payload(user_data)

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
      required_keys = ("last_name", "country_code", "postal_code")
      if not all(key in user_data for key in required_keys):
        missing_keys = user_data.keys() - required_keys
        print(
          "Raising exception because the following required mailing "
          f"address keys are missing: {missing_keys}"
        )
        raise ValueError(f"Missing mailing address fields:  {missing_keys}")

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
    return _USER_IDENTIFIER_FIELDS

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
