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

"""CM 360 OCI destination implementation."""

# pylint: disable=raise-missing-from

import datetime
import enum
import json
import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from googleapiclient import discovery
import httplib2
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

import errors
import immutabledict
import requests
from pydantic import Field
from utils import ProtocolSchema, RunResult, SchemaUtils, ValidationResult

# Filename used for the credential store.
CREDENTIAL_STORE_FILE = 'auth-file.dat'

# The OAuth 2.0 scopes to request.
OAUTH_SCOPES = ['https://www.googleapis.com/auth/dfareporting']

CM_CONVERSIONS_FIELDS = [
  "floodlightConfigurationId",
  "floodlightActivityId",
  "encryptedUserId",
  "mobileDeviceId",
  "timestamp_micros",
  "value",
  "quantity",
  "ordinal",
  "limitAdTracking",
  "childDirectedTreatment",
  "gclid",
  "nonPersonalizedAd",
  "treatmentForUnderage",
  "matchId",
  "dclid",
  "impressionId",
  "userIdentifiers",
  "kind",
]

class Destination:
  """Implements DestinationProto protocol for Campaign Manager Offline Conversion Import."""

  def __init__(self, config: Dict[str, Any]):
    self.config = config  # Keeping a reference for convenience.
    self.profileId = config.get("profile_id")
    self.client_secrets_file = config.get("client_secrets_file")
    self.client_secrets = config.get("secrets")
    f = open(self.client_secrets_file, "w")
    f.write(json.dumps(self.client_secrets))
    f.close()
    # Authenticate using the supplied user account credentials
    self.http = self.authenticate_using_user_account(self.client_secrets_file)
    self._validate_credentials()
    self.encryption_info = {}
    self.encryption_info["encryptionEntityType"] = config.get("encryptionEntityType")
    self.encryption_info["encryptionEntityId"] = config.get("encryptionEntityId")
    self.encryption_info["encryptionEntitySource"] = config.get("encryptionEntitySource")
    self.encryption_info["kind"] = config.get("kind")

  def authenticate_using_user_account(self,client_secrets_file):
    """Authorizes an httplib2.Http instance using user account credentials."""
    # Set up a Flow object to be used if we need to authenticate.
    flow = client.flow_from_clientsecrets(
        client_secrets_file, scope=OAUTH_SCOPES)

    # Check whether credentials exist in the credential store. Using a credential
    # store allows auth credentials to be cached, so they survive multiple runs
    # of the application. This avoids prompting the user for authorization every
    # time the access token expires, by remembering the refresh token.
    storage = Storage(CREDENTIAL_STORE_FILE)
    #credentials = storage.get()

    # If no credentials were found, go through the authorization process and
    # persist credentials to the credential store.
    #if credentials is None or credentials.invalid:
    credentials = tools.run_flow(flow, storage)

    # Use the credentials to authorize an httplib2.Http instance.
    http = credentials.authorize(httplib2.Http())

    return http
  
  def _validate_credentials(self) -> None:
    """Validate credentials.

    Raises:
      Exception: If credential combination does not meet criteria.
    """
    if not self.client_secrets_file:
      raise errors.DataOutConnectorValueError(
          f"Missing client secrets file in config: {self.config}"
      )

  def _parse_timestamp_micros(self, conversion: Dict[str, Any]):
    t = conversion.get("timestamp_micros")
    if t:
      timestamp_micros = int(t) if t.isdigit() else None
      return timestamp_micros
    return None

  '''def _validate_param(self, key: str, value: Any) -> bool:
    """Filter out null parameters and reserved keys."""
    reserved_keys = ['app_instance_id', 'client_id', 'user_id', 'timestamp_micros', 'event_name']
    result = key not in reserved_keys and value is not None and value != ''
    return result'''


  def _send_payload(self, payload: Dict[str, Any]) -> None:
    """Sends payload to GA via Measurement Protocol REST API.

    Args:
      payload: Parameters containing required data for app conversion tracking.

    Returns:
      results: Includes request body, status_code, error_msg, response body and
      debug flag.
    """

    authenticate_using_user_account(self.path_to_client_secrets_file)

    # Construct a service object via the discovery service.
    service = discovery.build('dfareporting', 'v4', http=http)

    try:
      request = service.conversions().batchinsert(profileId=self.profileId,
                                            body=payload)

      response = request.execute()
      # Success is to be considered between 200 and 299:
      # https://developer.mozilla.org/en-US/docs/Web/HTTP/Status
      if response.status_code < 200 or response.status_code >= 300:
        raise errors.DataOutConnectorSendUnsuccessfulError(
            msg="Sending payload to CM360 did not complete successfully.",
            error_num=errors.ErrorNameIDMap.RETRIABLE_CM360_HOOK_ERROR_HTTP_ERROR,
        )
    except requests.ConnectionError:
      raise errors.DataOutConnectorSendUnsuccessfulError(
          msg="Sending payload to CM360 did not complete successfully.",
          error_num=errors.ErrorNameIDMap.RETRIABLE_CM360_HOOK_ERROR_HTTP_ERROR,
      )

  def send_data(self, input_data: List[Dict[str, Any]], dry_run:bool):
    """Builds payload and sends data to CM360 API."""

    conversions = []
    encryption_info = {}

    for entry in input_data["conversions"]:
      conversion = {}
      for conversion_field in CM_CONVERSION_FIELDS:
        if conversion_field == "timestamp_micros":
          timestamp_micros = self._parse_timestamp_micros(entry)
          if timestamp_micros:
            conversion[conversion_field] = timestamp_micros
          else:
            conversion[conversion_field] = ""
        elif conversion_field == "conversion_kind":
          conversion["kind"] = str(entry.get(conversion_field, ""))
        else:
          conversion[conversion_field] = str(entry.get(conversion_field, ""))
      
      conversions.append(conversion)

    payload = {}
    payload["encryptionInfo"] = self.encryption_info
    payload["conversions"] = conversions

    success = True
    send_error = ""

    if not dry_run:
      try:
        self._send_payload(payload)
      except (
            errors.DataOutConnectorSendUnsuccessfulError,
        ) as error:
          send_error = error.error_num
    else:
      print(
        "Dry-Run: CM conversions event will not be sent."
      )

    run_result = RunResult(
        success=success,
        error_messages=send_error,
        dry_run=dry_run,
    )

    return True

  @staticmethod
  def schema() -> Optional[ProtocolSchema]:
    return ProtocolSchema(
        "CM360OCI",
        [
          ("client_id", str, Field(description="An OAuth2.0 Web Client ID.")),
          ("client_secret", str, Field(description="An OAuth2.0 Web Client Secret.")),
          ("profile_id", str, Field(description="A Camaign Manager Profile ID .")),
        ]
    )

  def fields(self) -> Sequence[str]:
    return CM_CONVERSIONS_FIELDS

  def batch_size(self) -> int:
    return 10000

