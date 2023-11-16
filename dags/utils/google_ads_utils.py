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
"""Utility functions for DAGs."""

from collections import defaultdict
import hashlib
import re
from typing import Any, Dict

from google.ads.googleads.client import GoogleAdsClient

from utils.validation_result import ValidationResult

_DEFAULT_GOOGLE_ADS_API_VERSION = "v14"

_REQUIRED_GOOGLE_ADS_CREDENTIALS = frozenset([
    "client_id", "client_secret", "developer_token", "login_customer_id",
    "refresh_token"
])


class GoogleAdsUtils:
  """Utility functions for Google Ads connectors."""
  PartialFailures = Dict[int, str]

  def validate_google_ads_config(self, config: dict[str,
                                                    Any]) -> ValidationResult:
    """Validates the provided config can build a Google Ads client.

    Args:
      config: The Tightlock config file.

    Returns:
      A ValidationResult for the provided config.
    """
    missing_fields = []
    for credential in _REQUIRED_GOOGLE_ADS_CREDENTIALS:
      if not config.get(credential, ""):
        missing_fields.append(credential)

    if missing_fields:
      error_msg = ("Config requires the following fields to be set: "
                   f"{', '.join(missing_fields)}")
      return ValidationResult(False, [error_msg])

    return ValidationResult(True, [])

  def build_google_ads_client(
      self,
      config: dict[str, Any],
      version: str = _DEFAULT_GOOGLE_ADS_API_VERSION) -> GoogleAdsClient:
    """Generate Google Ads Client.

    Requires the following to be stored in config:
    - client_id
    - client_secret
    - developer_token
    - login_customer_id
    - refresh_token

    Args:
      config: The Tightlock config file.
      version: (Optional) Version number for Google Ads API prefixed with v.

    Returns: Instance of GoogleAdsClient
    """
    credentials = {}

    for credential in _REQUIRED_GOOGLE_ADS_CREDENTIALS:
      credentials[credential] = config.get(credential, "")

    credentials["use_proto_plus"] = True

    return GoogleAdsClient.load_from_dict(config_dict=credentials,
                                          version=version)

  def get_partial_failures(self, client: GoogleAdsClient,
                           response: Any) -> PartialFailures:
    """Checks whether a response message has a partial failure error.

    In Python the partial_failure_error attr is always present on a response
    message and is represented by a google.rpc.Status message. So we can't
    simply check whether the field is present, we must check that the code is
    non-zero. Error codes are represented by the google.rpc.Code proto Enum:
    https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto

    Args:
        response:  A MutateAdGroupsResponse message instance.

    Returns: An empty dict if no partial failures exist, or a dict of the index
      index mapped to the error message.
    """
    partial_failure = getattr(response, "partial_failure_error", None)
    code = getattr(partial_failure, "code", None)
    if code == 0:
      # No failures.
      print("No partial failures found.")
      return {}

    error_details = getattr(partial_failure, "details", [])

    partial_failures = defaultdict(str)

    for error_detail in error_details:
      # Retrieve an instance of the GoogleAdsFailure class from the client
      failure_message = client.get_type("GoogleAdsFailure")
      # Parse the string into a GoogleAdsFailure message instance.
      # To access class-only methods on the message we retrieve its type.
      GoogleAdsFailure = type(failure_message)
      failure_object = GoogleAdsFailure.deserialize(error_detail.value)

      for error in failure_object.errors:
        index = error.location.field_path_elements[0].index
        message = f'Code: {error.error_code}, Error: {error.message}'
        partial_failures[
            index] += message  # Can be multiple errors for the same conversion.

    print(f"Partial failures: {partial_failures}")

    return partial_failures

  def normalize_and_hash_email_address(self, email_address: str) -> str:
    """Returns the result of normalizing and hashing an email address.

    For this use case, Google Ads (and the GMP suite) requires removal of any
    '.' characters preceding "gmail.com" or "googlemail.com".git 

    Args:
        email_address: An email address to normalize.

    Returns:
        A normalized (lowercase, removed whitespace) and SHA-265 hashed string.
    """
    normalized_email = email_address.lower()
    email_parts = normalized_email.split("@")
    # Checks whether the domain of the email address is either "gmail.com"
    # or "googlemail.com". If this regex does not match then this statement
    # will evaluate to None.
    is_gmail = re.match(r"^(gmail|googlemail)\.com$", email_parts[1])

    # Check that there are at least two segments and the second segment
    # matches the above regex expression validating the email domain name.
    if len(email_parts) > 1 and is_gmail:
      # Removes any '.' characters from the portion of the email address
      # before the domain if the domain is gmail.com or googlemail.com.
      email_parts[0] = email_parts[0].replace(".", "")
      normalized_email = "@".join(email_parts)

    return self.normalize_and_hash(normalized_email)

  def normalize_and_hash(self, s: str) -> str:
    """Normalizes and hashes a string with SHA-256.

    Private customer data must be hashed during upload, as described at:
    https://support.google.com/google-ads/answer/7474263

    Args:
        s: The string to perform this operation on.

    Returns:
        A normalized (lowercase, removed whitespace) and SHA-256 hashed string.
    """
    return hashlib.sha256(s.strip().lower().encode()).hexdigest()
