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

"""Google Ads EC for Web destination implementation."""
import errors

from collections import defaultdict
from google.ads.googleads.errors import GoogleAdsException
from pydantic import Field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from utils import GoogleAdsUtils, ProtocolSchema, RunResult, ValidationResult

_BATCH_SIZE = 10000

_DEFAULT_CURRENCY_CODE = "USD"

_REQUIRED_FIELDS = [
  "customer_id",
  "conversion_action_id",
  "order_id",
]

_ID_FIELDS = [
  "email",
  "phone_number",
  "hashed_email",
  "hashed_phone_number",
  "address_info" 
]

_OTHER_FIELDS = [
  "conversion_custom_variable_id",
  "conversion_custom_variable_value",
]

AdjustmentIndicesToAdjustments = List[Tuple[int, Any]]
CustomerAdjustmentMap = Dict[str, AdjustmentIndicesToAdjustments]
InvalidAdjustmentIndices = List[Tuple[int, errors.ErrorNameIDMap]]


class Destination:
  """Implements DestinationProto protocol for Google Ads EC for Web."""

  def __init__(self, config: Dict[str, Any]):
    """ Initializes Google Ads EC for Web Destination Class.

    Args:
      config: Configuration object to hold environment variables
    """
    self._config = config  # Keeping a reference for convenience.
    self._client = GoogleAdsUtils().build_google_ads_client(self._config)
    self._conversion_upload_service = self._client.get_service(
      "ConversionAdjustmentUploadService")

    print("Initialized Google Ads EC4W Destination class.")

  def send_data(
      self, input_data: List[Mapping[str, Any]], dry_run: bool
  ) -> Optional[RunResult]:
    """Builds payload and sends data to Google Ads API.

    Args:
      input_data: A list of rows to send to the API endpoint.
      dry_run: If True, will not send data to API endpoints.

    Returns: A RunResult summarizing success / failures, etc.
    """
    valid_adjustments, invalid_indices_and_errors = self._get_valid_and_invalid_adjustments(
      input_data
    )
    successfully_uploaded_adjustments = []

    if not dry_run:
      for customer_id, adjustment_data in valid_adjustments.items():
        adjustment_indices = [data[0] for data in adjustment_data]
        adjustments = [data[1] for data in adjustment_data]

        try:
          partial_failures = self._send_request(customer_id, adjustments)
        except GoogleAdsException as error:
          # Set every index as failed
          err_msg = error.error.code().name
          invalid_indices_and_errors.extend([(index, err_msg) for index in adjustment_indices])
        else:
          # Handles partial failures: Checks which adjustments were successfully
          # sent, and which failed.
          partial_failure_indices = set(partial_failures.keys())

          for index in range(len(adjustments)):
            # Maps index from this customer's adjustments back to original input data index.
            original_index = adjustment_indices[index]
            if index in partial_failure_indices:
              invalid_indices_and_errors.append((original_index, partial_failures[index]))
            else:
              successfully_uploaded_adjustments.append(original_index)
    else:
      print(
        "Dry-Run: Events will not be sent to the API."
      )

    print(f"Sent adjustments: {successfully_uploaded_adjustments}")
    print(f"Invalid events: {invalid_indices_and_errors}")

    for invalid_adjustment in invalid_indices_and_errors:
      adjustment_index = invalid_adjustment[0]
      error = invalid_adjustment[1]
      # TODO(b/272258038): TBD What to do with invalid events data.
      print(f"adjustment_index: {adjustment_index}; error: {error}")

    return RunResult(
      successful_hits=len(successfully_uploaded_adjustments),
      failed_hits=len(invalid_indices_and_errors),
      error_messages=[str(error[1]) for error in invalid_indices_and_errors],
      dry_run=dry_run,
    )

  def _get_valid_and_invalid_adjustments(
      self, adjustments: List[Mapping[str, Any]]
  ) -> Tuple[CustomerAdjustmentMap, InvalidAdjustmentIndices]:
    """Prepares the adjustment data for API upload.

    Args:
      adjustments: The adjustments data to upload.

    Returns:
      A dictionary of customer IDs mapped to index-adjustment tuples for the
      valid adjustments, and a list of index-error for the invalid adjustments.

    For example:
      valid_adjustments = {"customer1": [(1, "AdjustmentOne"), (2, "Adjustment2")]}
      invalid_indices_and_errors = [3, MISSING_MADATORY_FIELDS, ...]
    """
    valid_adjustments = defaultdict(list)
    invalid_indices_and_errors = []

    for i, adjustment in enumerate(adjustments):
      valid = True

      # Checks required fields set.
      for required_field in _REQUIRED_FIELDS:
        if not adjustment.get(required_field, ""):
          invalid_indices_and_errors.append((i, errors.ErrorNameIDMap.ADS_OC_HOOK_ERROR_MISSING_MANDATORY_FIELDS))
          valid = False

      if not valid:
        # Invalid conversion.
        continue

      # Builds the API adjustment payload.
      conversion_adjustment = self._client.get_type("ConversionAdjustment")
      conversion_action_service = self._client.get_service("ConversionActionService")

      customer_id = adjustment.get("customer_id")

      conversion_adjustment.conversion_action = conversion_action_service.conversion_action_path(
        customer_id, adjustment.get("conversion_action_id", "")
      )

      conversion_adjustment.adjustment_type = 'ENHANCEMENT'


      email = adjustment.get("email", "")
      phone_number = adjustment.get("phone_number", "")
      hashed_email = adjustment.get("hashed_email", "")
      hashed_phone_number = adjustment.get("hashed_phone_number", "")
      address_info = adjustment.get("adress_info", "")
      order_id = adjustment.get("order_id", "")

      # Sets the order ID if provided.
      conversion_adjustment.order_id = order_id

      # Populates user_identifier fields
      user_identifier = self._client.get_type("UserIdentifier")
      if hashed_email:
        user_identifier.hashed_email = hashed_email
      elif email:
        user_identifier.hashed_email = GoogleAdsUtils().normalize_and_hash_email_address(email)
      
      if hashed_phone_number:
        user_identifier.hashed_phone_number = hashed_phone_number
      elif phone_number:
        user_identifier.hashed_phone_number = GoogleAdsUtils().normalize_and_hash(phone_number)

      if address_info:
        # TODO(caiotomazelli): Verify how to populate address info with each one of the proto fields
        user_identifier.adress_info = address_info

      # Specifies the user identifier source.
      user_identifier.user_identifier_source = (
          self._client.enums.UserIdentifierSourceEnum.FIRST_PARTY
      )

      conversion_adjustment.user_identifiers.append(user_identifier)

      conversion_custom_variable_id = adjustment.get("conversion_custom_variable_id", "")
      conversion_custom_variable_value = adjustment.get("conversion_custom_variable_value", "")

      # Adds custom variable ID and value if set.
      if conversion_custom_variable_id and conversion_custom_variable_value:
        conversion_custom_variable = self._client.get_type("CustomVariable")
        conversion_custom_variable.conversion_custom_variable = self._conversion_upload_service.conversion_custom_variable_path(
          customer_id, conversion_custom_variable_id
        )
        conversion_custom_variable.value = conversion_custom_variable_value
        conversion_adjustment.custom_variables.append(conversion_custom_variable)

      valid_adjustments[customer_id].append((i, conversion_adjustment))

    return valid_adjustments, invalid_indices_and_errors

  def _send_request(self, customer_id: str, adjustments: List[Any]) -> GoogleAdsUtils.PartialFailures:
    """Sends conversions to the offline conversion import API.

    Args:
      customer_id: The customer ID for these conversions.
      conversions: A list of click conversions.

    Returns: An empty dict if no partial failures exist, or a dict of the index
      mapped to the error message.
    """
    request = self._client.get_type("UploadConversionAdjustmentsRequest")
    request.customer_id = customer_id
    request.conversion_adjustments.extend(adjustments)
    request.partial_failure = True

    try:
      adjustment_upload_response = self._conversion_upload_service.upload_conversion_adjustments(
        request=request,
      )
    except GoogleAdsException as error:
      print(f"Caught GoogleAdsException: {error}")
      raise

    return GoogleAdsUtils().get_partial_failures(self._client, adjustment_upload_response)


  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    """Returns the required metadata for this destination config.

    Returns:
      An optional ProtocolSchema object that defines the
      required and optional metadata used by the implementation
      of this protocol.
    """
    return ProtocolSchema(
      "GADS_EC4WEB",
      [
        ("client_id", str, Field(description="An OAuth2.0 Web Client ID.")),
        ("client_secret", str, Field(description="An OAuth2.0 Web Client Secret.")),
        ("developer_token", str, Field(description="A Google Ads Developer Token.")),
        ("login_customer_id", str, Field(description="A Google Ads Login Customer ID (without hyphens).")),
        ("refresh_token", str, Field(description="A Google Ads API refresh token.")),
      ]
    )

  def fields(self) -> Sequence[str]:
    """Lists required fields for the destination input data.

    Returns:
      A sequence of fields.
    """
    return _REQUIRED_FIELDS + _ID_FIELDS + _OTHER_FIELDS

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

