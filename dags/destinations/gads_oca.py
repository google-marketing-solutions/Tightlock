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

"""Google Ads Conversion Adjustments destination implementation."""
import errors

from collections import defaultdict
from google.ads.googleads.errors import GoogleAdsException
from pydantic import Field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from utils import GoogleAdsUtils, ProtocolSchema, RunResult, ValidationResult, AdsPlatform

_BATCH_SIZE = 2000

_DEFAULT_CURRENCY_CODE = "USD"

_REQUIRED_FIELDS = [
    "customer_id",
    "conversion_action_id",
    "adjustment_date_time",
    "adjusted_value"
]

_OTHER_FIELDS = [
    "gclid",
    "conversion_date_time",
    "currency_code",
    "order_id",
]

AdjustmentIndicesToAdjustments = List[Tuple[int, Any]]
CustomerAdjustmentMap = Dict[str, AdjustmentIndicesToAdjustments]
InvalidAdjustmentIndices = List[Tuple[int, errors.ErrorNameIDMap]]


class Destination:
  """Implements DestinationProto protocol for Google Ads Conversion Adjustments."""

  def __init__(self, config: Dict[str, Any]):
    """ Initializes Google Ads Conversion Adjustments Destination Class.

    Args:
      config: Configuration object to hold environment variables
    """
    self._config = config  # Keeping a reference for convenience.
    self._client = GoogleAdsUtils().build_google_ads_client(self._config)
    self._conversion_upload_service = self._client.get_service(
      "ConversionAdjustmentUploadService")

    print("Initialized Google Ads OCA Destination class.")

  def send_data(
      self, input_data: List[Mapping[str, Any]], dry_run: bool
  ) -> Optional[RunResult]:
    """Builds payload and sends data to Google Ads API.

    Args:
      input_data: A list of rows to send to the API endpoint.
      dry_run: If True, will not send data to API endpoints.

    Returns: A RunResult summarizing success / failures, etc.
    """
    return GoogleAdsUtils().send_ads_conversions(
        get_valid_and_invalid_conversions=self._get_valid_and_invalid_conversions,
        send_request=self._send_request,
        input_data=input_data,
        dry_run=dry_run,
        ads_platform=AdsPlatform.GADS_OCA,
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

      conversion_adjustment.adjustment_type = 'RESTATEMENT'

      order_id = adjustment.get("order_id", "")
      gclid = adjustment.get("gclid", "")
      conversion_date_time = adjustment.get("conversion_date_time", "")
      adjustment_date_time = adjustment.get("adjustment_date_time", "")
      
      # Make sure that "falsy" values still default to _DEFAULT_CURRENCY_CODE
      restatement_value = self._client.get_type("RestatementValue")
      adjusted_value = float(adjustment.get("adjusted_value", ""))
      currency_code = adjustment.get("currency_code", False) or _DEFAULT_CURRENCY_CODE

      # Sets the order ID if provided.
      if order_id:
        conversion_adjustment.order_id = order_id

      # Specifies optional fields
      if gclid and conversion_date_time:
        gclid_date_time_pair = self._client.get_type("GclidDateTimePair")
        gclid_date_time_pair.gclid = gclid
        gclid_date_time_pair.conversion_date_time = conversion_date_time

        conversion_adjustment.gclid_date_time_pair = gclid_date_time_pair


      restatement_value = self._client.get_type("RestatementValue")
      restatement_value.adjusted_value = adjusted_value
      restatement_value.currency_code = currency_code

      conversion_adjustment.restatement_value = restatement_value
      conversion_adjustment.adjustment_date_time = adjustment_date_time


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
      "GADS_OCA",
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
    return _REQUIRED_FIELDS + _OTHER_FIELDS

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

