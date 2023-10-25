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

import errors
import logging

from utils import GoogleAdsUtils,ProtocolSchema, RunResult, ValidationResult


# Default batch size of 1000 user identifiers
_BATCH_SIZE = 1000


class Destination:
  """Implements DestinationProto protocol for Google Ads Customer Match.
  
  This destination assumes that the data source has 1 row per user, and 
  that each row can have multiple user identifiers set (ie email address and 
  phone number).  This destination will then generate 1-or-more user 
  identifiers to add to the user data upload job.

  Hence, not all of the fields specified by this destination are requred,
  but at least one of them must be supplied.
  """

  def __init__(self, config: Dict[str, Any]):
    """ Initializes Google Ads OCI Destination Class.

    Args:
      config: Configuration object to hold environment variables
    """
    self._config = config
    self._debug = config.get("debug", False)
    self._client = GoogleAdsUtils().build_google_ads_client(self._config)
    self._offline_user_data_job_service = self._client.get_service(
        "OfflineUserDataJobService"
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
    print(f"Processing {len(input_data)} user records")
    for user_data in input_data:
      user_operation = self._build_user_data_operation_from_row(user_data)
      user_data_operations.append(user_operation)

    # No point in continuing if doing a dry run, since the above steps
    #   detail how many rows were processed, and how many operations
    #   were created
    if (dry_run):
      print("Running as a dry run, so skipping upload steps.")
      return RunResult(dry_run=dry_run, successful_hits=0, failed_hits=0)

    upload_job_id = self._create_user_upload_job()
    add_user_data_request = self._create_add_user_data_request(
      upload_job_id, user_data_operations
    )

    response = offline_user_data_job_service_client.add_offline_user_data_job_operations(
        request=request
    )
    failures = self._process_response_for_failures(response)

    self._offline_user_data_job_service.run_offline_user_data_job(
      resource_name=upload_job_id
    )

    return RunResult(
      successful_hits=len(user_data_operations) - len(failures),
      failed_hits=len(failures),
      error_messages=failures
    )

  def _build_user_data_operation_from_row(user_data: Mapping[str, Any]) -> Any:
    """Builds a Google Ads OfflineUserDataJobOperation object from the provided user data.
    
    Args:
        user_data:  A row of user data from the Source.
    
    Returns:
        A Google Ads OfflineUserDataJobOperation object populated with 1-or-more identifiers.
    """
    user_data_payload = client.get_type("UserData")
    self._scrub_user_data(user_data)
    self._populate_payload_with_identifiers(user_data_payload, user_data)

    operation = client.get_type("OfflineUserDataJobOperation")
    operation.create = user_data
    return operation

  def _scrub_user_data(user_data: Mapping[str, Any]) -> None:
    """Scrub and format the user data to prepare it for the API."""
    pass

  def _populate_payload_with_identifiers(payload: Any, ser_data: Mapping[str, Any]) -> None:
    """Populates the provided Google Ads UserData payload object with source data."""
    pass

  def _create_user_upload_job() -> str:
    """Sends a request to create a user data upload job.

    Returns:
      The resource name ID (ie, 'jobs/some_id') of the created job
    """
    return ""

  def _create_add_user_data_request(upload_job_id: str, user_operations: List[Any]) -> Any:
    # Create and send a request to add the operations to the job
    request = client.get_type("AddOfflineUserDataJobOperationsRequest")
    request.resource_name = upload_job_id
    request.operations = user_data_operations
    request.enable_partial_failure = True

    return request

  def _process_response_for_failures(response: Any) -> List[str]:
    """Processes the response:  prints partial error details, and returns error details.
    
    Args:
      response:  The Google Ads AddOfflineUserDataJobOperationsResponse returned by
        the API call to add user operations to the job.

    Returns:
      List of error details, one message per failed user data operation.
    """
    return []

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
        ("client_id", str, Field(description="An OAuth2.0 Web Client ID.")),
        ("client_secret", str, Field(description="An OAuth2.0 Web Client Secret.")),
        ("developer_token", str, Field(description="A Google Ads Developer Token.")),
        ("login_customer_id", str, Field(description="A Google Ads Login Customer ID (without hyphens).")),
        ("refresh_token", str, Field(description="A Google Ads API refresh token.")),

        # User Data List variables
        ("user_list_id", str, Field(description="ID of the user list this destination will add users to.")),
        ("user_data_consent_status", 
         str, 
         Field(description="Data consent status for ad user data for all members in in this list")),
        ("user_personalization_consent_status", 
         str, 
         Field(description="Personalization consent status for ad user data for all members in in this list")),
      ]
    )

  def fields(self) -> Sequence[str]:
    """Lists required fields for the destination input data.

    Returns:
      A sequence of fields.
    """

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