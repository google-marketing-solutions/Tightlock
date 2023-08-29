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

"""Google Ads OCI destination implementation."""

# pylint: disable=raise-missing-from

import datetime
import enum
import json
import logging
from collections import defaultdict
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import errors
import immutabledict
import requests
from pydantic import Field
from utils import DagUtils, ProtocolSchema, RunResult, SchemaUtils, ValidationResult

_BATCH_SIZE = 10000

_DEFAULT_CURRENCY_CODE = "USD"

_REQUIRED_FIELDS = [
  "customer_id",
  "conversion_action_id",
  "conversion_date_time",
  "conversion_value",
]

_ID_FIELDS = [
  "gclid",
  "gbraid",
  "wbraid",
]


class Destination:
  """Implements DestinationProto protocol for Google Ads OCI."""

  def __init__(self, config: Dict[str, Any]) -> None:
    """ Initializes Google Ads OCI Destination Class.

    Args:
      config: Configuration object to hold environment variables
    """
    self._config = config  # Keeping a reference for convenience.
    self._client = DagUtils().build_google_ads_client(self._config)
    self._debug = config.get("debug", False)

    print("Initialized Google Ads OCI Destination class.")

  def send_data(
      self, input_data: List[Mapping[str, Any]], dry_run: bool
  ) -> Optional[RunResult]:
    """Builds payload and sends data to Google Ads API.

    Args:
      input_data: A list of rows to send to the API endpoint.

    Returns: A RunResult summarizing success / failures, etc.
    """
    valid_conversions, invalid_indices_and_errors = self._get_valid_and_invalid_conversions(
        input_data
    )

    # TODO: Batch here? Or will Tightlock do batching to this method?
    if not dry_run:
      for customer_id, conversion_data in valid_conversions.items():
        try:
          # TODO: Conversion data is a list of conversions for this customer ID
          indices = [data[0] for data in conversion_data]
          conversions = [data[1] for data in conversion_data]
          self._send_request(customer_id, conversions)
        except (
            errors.DataOutConnectorSendUnsuccessfulError,
            errors.DataOutConnectorValueError,
        ) as error:
          index = valid_event[0]
          invalid_indices_and_errors.append((index, error.error_num))
    else:
      print(
          "Dry-Run: Events will not be sent to the API."
      )

    print(f"Valid conversions: {valid_conversions}")
    print(f"Invalid events: {invalid_indices_and_errors}")

    for invalid_conversion in invalid_indices_and_errors:
      event_index = invalid_conversion[0]
      error_num = invalid_conversion[1]
      # TODO(b/272258038): TBD What to do with invalid events data.
      print(f"event_index: {event_index}; error_num: {error_num}")

    return RunResult(
        successful_hits=len(valid_events),
        failed_hits=len(invalid_indices_and_errors),
        error_messages=[str(error[1]) for error in invalid_indices_and_errors],
        dry_run=dry_run,
    )

  def _get_valid_and_invalid_conversions(
      self, offline_conversions: List[Dict[str, Any]]
  ) -> Dict[str, List[Tuple[int, Any]]], List[Tuple[int, errors.ErrorNameIDMap]]]:
    """Prepares the offline conversion data for API upload.

    Args:
      offline_conversions: The offline conversion data to upload..

    Returns:
      A dictionary of customer IDs mapped to index-conversion tuples for the
      valid conversions, and a list of index-error for the invalid conversions.

    For example:
      valid_conversions = {"customer1": [(1, "ConversionOne"), (2, "Conversion2")]}
      invalid_indices_and_errors = [3, MISSING_MADATORY_FIELDS, ...]
    """
    valid_conversions = defaultdict(list)
    invalid_indices_and_errors = []

    for i, conversion in enumerate(offline_conversions):
      valid = True

      # Checks required fields set.
      for required_field in _REQUIRED_FIELDS:
        if not conversion.get(required_field, ""):
          invalid_indices_and_errors.append((i, errors.ErrorNameIDMap.ADS_OC_HOOK_ERROR_MISSING_MANDATORY_FIELDS))
          valid = False

      # Checks exactly one of glid, gbraid, or braid is set.
      target_ids = [
        conversion.get(key, "") for key in _ID_FIELDS if conversion.get(key, "")]

      if len(target_ids) != 1:
        invalid_indices_and_errors.append((i, errors.ErrorNameIDMap.ADS_OC_HOOK_ERROR_INVALID_ID_CONFIG))
        valid = False

      if not valid:
        # Invalid conversion.
        continue

      # Builds the API click payload.
      click_conversion = self._client.get_type("ClickConversion")
      conversion_upload_service = self._client.get_service("ConversionUploadService")
      conversion_action_service = self._client.get_service("ConversionActionService")

      customer_id = conversion.get("customer_id")

      click_conversion.conversion_action = conversion_action_service.conversion_action_path(
        customer_id, conversion.get("conversion_action_id", "")
      )

      gclid = conversion.get("gclid", "")
      gbraid = conversion.get("gbraid", "")
      wbraid = conversion.get("wbraid", "")

      # Sets the single specified ID field.
      if gclid:
        click_conversion.gclid = gclid
      elif gbraid:
        click_conversion.gbraid = gbraid
      else:
        click_conversion.wbraid = wbraid

      click_conversion.conversion_value = float(conversion.get("conversion_value", ""))
      click_conversion.conversion_date_time = conversion.get("conversion_date_time", "")
      click_conversion.currency_code = conversion.get("conversion_date_time", _DEFAULT_CURRENCY_CODE)

      conversion_customer_variable_id = conversion.get("conversion_custom_variable_id", "")
      conversion_custom_variable_value = conversion.get("conversion_custom_variable_value", "")

      # Adds custom variable ID and value if set.
      if conversion_custom_variable_id and conversion_custom_variable_value:
        conversion_custom_variable = self._client.get_type("CustomVariable")
        conversion_custom_variable.conversion_custom_variable = conversion_upload_service.conversion_custom_variable_path(
          customer_id, conversion_custom_variable_id
        )
        conversion_custom_variable.value = conversion_custom_variable_value
        click_conversion.custom_variables.append(conversion_custom_variable)

      valid_conversions[customer_id].append((i, click_conversion))

    return valid_conversions, invalid_indices_and_errors

  def _send_request(self, customer_id: str, conversions: List[Any]) -> None:
    """Sends conversions to the offline conversion import API.

    Args:
      customer_id: The customer ID for these conversions.
      conversions: A list of click conversions.
    """
    request = self._client.get_type("UploadClickConversionsRequest")
    request.customer_id = customer_id
    request.conversions.extend(conversions)
    request.partial_failure = True
    conversion_upload_response = conversion_upload_service.upload_click_conversions(
      request=request,
    )

    uploaded_conversion = conversion_upload_response.results[0]

    # TODO: Check and handle errors.
    # TODO: How to handle partial failures?

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    """Returns the required metadata for this destination config.

    Returns:
      An optional ProtocolSchema object that defines the
      required and optional metadata used by the implementation
      of this protocol.
    """
    return ProtocolSchema(
      "GADS_OCI",
      [
        ("customer_id", str, Field(description="The Google Ads customer ID.")),
        ("conversion_action_id", str, Field(
          description="The conversion action ID.")),
        ("conversion_date_time", str, Field(description=(
          "The date and time of the conversion (should be after the click "
          "time). The format is 'yyyy-mm-dd hh:mm:ss+|-hh:mm, e.g. "
          "'2019-01-01 12:32:45-08:00'"))),
        ("conversion_value", str, Field(description="The conversion value.")),
        ("conversion_custom_variable_id", Optional[str], Field(default=None,
        description=("The ID of the conversion custom variable to associate "
        "with the upload."))),
        ("currency_code", Optional[str], Field(
          default=_DEFAULT_CURRENCY_CODE, description=("The currency code "
          "for the value of this conversion."))),
        ("conversion_custom_variable_value", Optional[str], Field(
          default=None, description=("The value of the conversion custom "
          "variable to associate with the upload."))),
        ("gclid", Optional[str], default=None, Field(description=(
          "The Google Click Identifier (gclid) which should be newer than "
          "the number of days set on the conversion window of the conversion "
          "action. Only one of either a gclid, WBRAID, or GBRAID identifier can "
          "be passed into this example. See the following for more details: "
          "https://developers.google.com/google-ads/api/docs/conversions/upload-clicks"))),
        ("gbraid", Optional[str], default=None, Field(description=(
          "The GBRAID identifier for an iOS app conversion. Only one of "
          "either a gclid, WBRAID, or GBRAID identifier can be passed into this "
          "example. See the following for more details: "
          "https://developers.google.com/google-ads/api/docs/conversions/upload-clicks"))),
        ("wbraid", Optional[str], Field(default=None, description=(
          "The WBRAID identifier for an iOS app conversion. Only one of "
          "either a gclid, WBRAID, or GBRAID identifier can be passed into this "
          "example. See the following for more details: "
          "https://developers.google.com/google-ads/api/docs/conversions/upload-clicks"))),
      ]
    )

  def fields(self) -> Sequence[str]:
    """Lists required fields for the destination input data.

    Returns:
      A sequence of fields.
    """
    return _REQUIRED_FIELDS

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
    return utils.DagUtils().validate_google_ads_config(self._config)
