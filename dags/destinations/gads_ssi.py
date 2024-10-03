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

"""Google Ads SSI destination implementation."""
import errors

from collections import defaultdict
from google.ads.googleads.errors import GoogleAdsException
from pydantic import Field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from utils import GoogleAdsUtils, ProtocolSchema, RunResult, ValidationResult, AdsPlatform

_BATCH_SIZE = 5000

_DEFAULT_CURRENCY_CODE = "USD"

_STORE_SALES_UPLOAD_TARGET = "STORE_SALES_UPLOAD_FIRST_PARTY"

_LOYALTY_FRACTION = 1.0

_TRANSACTION_UPLOAD_FRACTION = 1.0

_REQUIRED_FIELDS = [
    "customer_id",
    "conversion_action_id",
    "conversion_date_time",
    "conversion_micro_value",
]

_ID_FIELDS = [
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
    "state",
    "city",
]

_OTHER_FIELDS = [
    "external_id",
    "currency_code",
    "custom_key",
    "custom_value",
    "ad_user_data_consent",
    "ad_personalization_consent",
]

ConversionIndicesToConversions = List[Tuple[int, Any]]
CustomerConversionMap = Dict[str, ConversionIndicesToConversions]
InvalidConversionIndices = List[Tuple[int, errors.ErrorNameIDMap]]


class Destination:
  """Implements DestinationProto protocol for Google Ads Store Sales Improvement"""

  def __init__(self, config: Dict[str, Any]):
    """Initializes Google Ads SSI Class.

    Args:
      config: Configuration object to hold environment variables
    """
    self._config = config  # Keeping a reference for convenience.
    self._client = GoogleAdsUtils().build_google_ads_client(self._config)
    self._offline_data_job_service = self._client.get_service(
        "OfflineUserDataJobService"
    )

    print("Initialized Google Ads SSI Destination class.")

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
        ads_platform=AdsPlatform.GADS_SSI,
    )

  def _get_user_identifiers(
      self, conversion: Mapping[str, Any]
  ) -> List[Dict[str, Any]]:
    user_identifiers = []

    if "email" in conversion:
      user_identifier = self._client.get_type("UserIdentifier")
      user_identifier.hashed_email = (
          GoogleAdsUtils().normalize_and_hash_email_address(
              conversion.get("email")
          )
      )
      user_identifiers.append(user_identifier)

    elif "hashed_email" in conversion:
      user_identifier = self._client.get_type("UserIdentifier")
      user_identifier.hashed_email = conversion.get("hashed_email")

      user_identifiers.append(user_identifier)

    if "phone" in conversion:
      user_identifier = self._client.get_type("UserIdentifier")
      user_identifier.hashed_phone_number = GoogleAdsUtils().normalize_and_hash(
          conversion.get("phone")
      )
      user_identifiers.append(user_identifier)

    elif "hashed_phone" in conversion:
      user_identifier = self._client.get_type("UserIdentifier")
      user_identifier.hashed_phone_number = conversion.get("phone")
      user_identifiers.append(user_identifier)

    if "country_code" in conversion and "postal_code" in conversion:
      if "first_name" in conversion or "hashed_first_name" in conversion:
        user_identifier = self._client.get_type("UserIdentifier")

        if "first_name" in conversion:
          address_info = user_identifier.address_info
          address_info.hashed_first_name = conversion.get("first_name")
          address_info.hashed_last_name = conversion.get("last_name")

        elif "hashed_first_name" in conversion:
          address_info = user_identifier.address_info
          address_info.hashed_first_name = (
              GoogleAdsUtils().normalize_and_hash(
                  conversion.get("first_name")
              )
          )
          address_info.hashed_last_name = GoogleAdsUtils().normalize_and_hash(
              conversion.get("last_name")
          )

        address_info.country_code = conversion.get("country_code")
        address_info.postal_code = conversion.get("postal_code")
        user_identifiers.append(user_identifier)

    return user_identifiers

  def _get_valid_and_invalid_conversions(
      self, offline_conversions: List[Mapping[str, Any]]
  ) -> Tuple[CustomerConversionMap, InvalidConversionIndices]:
    """Prepares the offline conversion data for API upload.

    Args:
      offline_conversions: The offline conversion data to upload.

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
          invalid_indices_and_errors.append(
              (
                  i,
                  errors.ErrorNameIDMap.ADS_SSI_HOOK_ERROR_MISSING_MANDATORY_FIELDS,
              )
          )
          valid = False

      if not valid:
        # Invalid conversion.
        continue

      user_data_operation = self._client.get_type("OfflineUserDataJobOperation")
      user_data_create_operation = user_data_operation.create

      conversion_action_service = self._client.get_service(
          "ConversionActionService"
      )

      customer_id = conversion.get("customer_id")
      conversion_action_resource_name = (
          conversion_action_service.conversion_action_path(
              customer_id, conversion.get("conversion_action_id", "")
          )
      )

      # Populates user_identifier fields
      user_identifiers = self._get_user_identifiers(conversion)
      user_data_create_operation.user_identifiers.extend(user_identifiers)
      transaction_attribute = user_data_create_operation.transaction_attribute
      transaction_attribute.conversion_action = conversion_action_resource_name
      transaction_attribute.currency_code = (
          conversion.get("currency_code") or _DEFAULT_CURRENCY_CODE
      )
      transaction_attribute.transaction_amount_micros = int(
          conversion.get("conversion_micro_value", 1)
      )
      transaction_attribute.transaction_date_time = conversion.get(
          "conversion_date_time", ""
      )

      custom_key = self._config.get("custom_key", "")
      custom_value = conversion.get("custom_value", "")

      # Adds custom variable ID and value if set.
      if custom_key and custom_value:
        transaction_attribute.custom_value = custom_value

      ad_user_data = conversion.get("ad_user_data_consent", "")
      ad_personalization = conversion.get("ad_personalization_consent", "")

      if ad_user_data:
        user_data_create_operation.consent.ad_user_data = ad_user_data

      if ad_personalization:
        user_data_create_operation.consent.ad_personalization = (
            ad_personalization
        )

      valid_conversions[customer_id].append((i, user_data_operation))

    return valid_conversions, invalid_indices_and_errors

  def _create_offline_data_job(
      self,
      customer_id: str,
  ) -> str:
    """Creates an offline, user data job.

    Args:
      customer_id: The customer ID for these store conversions.

    Returns: the resource_name as a string for the job created
    """

    offline_user_data_job = self._client.get_type("OfflineUserDataJob")
    offline_user_data_job.type_ = _STORE_SALES_UPLOAD_TARGET

    external_id = self._config.get("external_id", "")
    if external_id:
      offline_user_data_job.external_id = external_id

    store_sales_metadata = offline_user_data_job.store_sales_metadata
    store_sales_metadata.loyalty_fraction = _LOYALTY_FRACTION
    store_sales_metadata.transaction_upload_fraction = _TRANSACTION_UPLOAD_FRACTION

    custom_key = self._config.get("custom_key", "")
    if custom_key:
      store_sales_metadata.custom_key = custom_key

    response = self._offline_data_job_service.create_offline_user_data_job(
        customer_id=customer_id, job=offline_user_data_job
    )

    return response.resource_name

  def _send_request(
      self, customer_id: str, conversions: List[Any]
  ) -> GoogleAdsUtils.PartialFailures:
    """Sends conversions to the offline conversion import API.

    Args:
      customer_id: The customer ID for these conversions.
      conversions: A list of click conversions.

    Returns: An empty dict if no partial failures exist, or a dict of the index
      mapped to the error message.
    """

    try:
      request = self._client.get_type("AddOfflineUserDataJobOperationsRequest")
      request.resource_name = self._create_offline_data_job(customer_id)
      request.operations = conversions
      request.enable_partial_failure = True
      request.enable_warnings = True

      response = (
          self._offline_data_job_service.add_offline_user_data_job_operations(
              request=request,
          )
      )

      self._offline_data_job_service.run_offline_user_data_job(
          resource_name=request.resource_name
      )
      
    except GoogleAdsException as error:
      print(f"Caught GoogleAdsException: {error}")
      raise

    return GoogleAdsUtils().get_partial_failures(self._client, response)

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    """Returns the required metadata for this destination config.

    Returns:
      An optional ProtocolSchema object that defines the
      required and optional metadata used by the implementation
      of this protocol.
    """
    return ProtocolSchema(
        "GADS_SSI",
        [
            ("client_id", str, Field(description="An OAuth2.0 Web Client ID.")),
            (
                "client_secret",
                str,
                Field(description="An OAuth2.0 Web Client Secret."),
            ),
            (
                "developer_token",
                str,
                Field(description="A Google Ads Developer Token."),
            ),
            (
                "login_customer_id",
                str,
                Field(
                    description="A Google Ads Login Customer ID (without hyphens)."
                ),
            ),
            (
                "refresh_token",
                str,
                Field(description="A Google Ads API refresh token."),
            ),
            (
                "external_id",
                Optional[str],
                Field(
                    default="",
                    description="Optional, but recommended, external ID for the offline user data job."
                ),
            ),
            (
                "custom_key",
                Optional[str],
                Field(
                    default="",
                    description="A custom key str to segment store sales conversions. Only required after creating a custom key and custom values in the account."
                ),
            ),
        ],
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
