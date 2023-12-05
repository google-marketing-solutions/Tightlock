# coding=utf-8
# python3

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


"""Errors file for this data connector component.

All exceptions defined by the library should be in this file.
"""
import enum
from typing import Optional

import immutabledict

# A dictionary with error numbers and error descriptions to use for consistent
# logging and error handling across TCRM.
_ERROR_ID_DESCRIPTION_MAP = immutabledict.immutabledict({
    10: 'General error occurred.',
    11: 'Event not sent due to authentication error. Event is due for retry.',
    12: 'Event not sent. Event is due for retry.',
    13: "Event not sent to Google Ads. Couldn't get service from Google Adwords API",
    14: 'Error in sending event to Google Ads. Failed to create user list with response error.',
    15: 'Error in sending event to Google Ads. Failed to add members to the user list.',
    16: 'Event not sent to Google Ads UAC. Fail to get airflow connection.',
    17: 'Event not sent to Google Ads UAC. Missing dev token in connection password.',
    18: 'Error in loading events from BigQuery. Http error.',
    19: 'Error in loading events from BigQuery. Unable to get total rows.',
    20: 'Error in sending event to Google Analytics. Http error.',
    21: 'Error in loading events from Google Cloud Storage. Http error.',
    22: 'Error in sending event to Google Analytics 4. Http error.',
    23: 'Error in sending conversion to Campaign Manager 360. Http error.',
    24: 'Error in sending audience lists to DV360. Http error.',
    50: 'Event not sent. Event will not be retried.',
    51: 'Error in sending event to Ads Customer Match. Hashed values in the payload do not match SHA256 format.',
    52: 'Error in sending event to Ads Customer Match. HashedEmail field does not meet SHA256 format.',
    53: 'Error in sending event to Ads Customer Match. HashedPhoneNumber field does not meet SHA256 format.',
    54: 'Error in sending event to Ads Customer Match. Email and phone number field are invalid.',
    55: 'Error in sending event to Ads Customer Match. UserId does not exist in crm_id event.',
    56: 'Error in sending event to Ads Customer Match. MobileId does not exist in the event.',
    57: 'Error in sending event to Ads Customer Match. User list name is empty.',
    58: 'Error in sending event to Ads Customer Match. Membership lifespan is not between 0 and 10,000.',
    59: 'Error in sending event to Ads Customer Match. Invalid upload key type. Refer to ads_hook.UploadKeyType for details.',
    60: 'Error in sending event to Ads Customer Match. app_id needs to be specified for MOBILE_ADVERTISING_ID when create_list is True.',
    61: 'Error in sending event to Ads Customer Match. Not creating list when user list name does not exist.',
    62: 'Error in sending event to Ads API. Failed to get user list ID.',
    63: 'Error in sending event to Ads Offline Conversion. Event is missing at least one mandatory field(s).',
    64: 'Error in sending event to Ads Offline Conversion. Length of conversionName should be <= 100.',
    65: 'Error in sending event to Ads Offline Conversion. ConversionTime should be formatted: yyyymmdd hhmmss [tz]',
    66: 'Error in sending event to Ads Offline Conversion. ConversionValue should be greater than or equal to 0.',
    67: 'Error in sending event to Ads Offline Conversion. Length of googleClickId should be between 1 and 512.',
    68: 'Error in sending event to Ads UAC. Event is missing at least one mandatory field(s).',
    69: 'Error in sending event to Ads UAC. Unsupported app event type in payload. Example: "first_open", "session_start", "in_app_purchase", "view_item_list", "view_item", "view_search_results", "add_to_cart", "ecommerce_purchase", "custom".',
    70: "Error in sending event to Ads UAC. App event type must be 'custom' when app event name exists.",
    71: 'Error in sending event to Ads UAC. Wrong raw device id format in payload. Should be compatible with RFC4122.',
    72: "Error in sending event to Ads UAC. Wrong raw device id type in payload. Example: 'advertisingid', 'idfa'.",
    73: 'Error in sending event to Ads UAC. Wrong limit-ad-tracking status in payload. Example: 0, 1.',
    74: 'Error in loading events from BigQuery. Unable to get any blobs.',
    75: 'Error in sending event to Campaign Manager. Event is missing at least one mandatory field(s).',
    76: 'Error in sending event to Campaign Manager. Value in customVariables list is invalid.',
    77: 'Error in sending event to Campaign Manager. Invalid conversion event.',
    78: 'Error in sending event to Google Analytics. Unsupported hit type.',
    79: 'Error in sending event to Google Analytics. Hit must have cid or uid.',
    80: 'Error in sending event to Google Analytics. Batch hits must be under 20.',
    81: 'Error in sending event to Google Analytics. Hit size exceeds limitation.',
    82: 'Error in sending event to Google Analytics. Invalid Tracking ID Format. The expected format is `UA-XXXXX-Y`.',
    83: 'Error in sending event to Google Analytics. Wrong send type.',
    84: 'Error in loading events from Google Cloud Storage. Invalid GCS blob content type.',
    85: 'Error in loading events from Google Cloud Storage. Failed to download the blob.',
    86: 'Error in loading events from Google Cloud Storage. Failed to parse the blob as JSON.',
    87: 'Error in loading events from Google Cloud Storage. Failed to parse the blob as CSV.',
    88: 'Error in loading events from Google Cloud Storage. Failed to parse CSV, not all lines have same length.',
    89: 'Missing or empty monitoring parameters although monitoring is enabled.',
    90: 'Error in sending event to Google Analytics 4. Payload is not valid JSON.',
    91: 'Error in sending event to Google Analytics 4. Invalid event value(s).',
    92: 'Error in sending event to Google Analytics 4. client_id is required.',
    93: 'Error in sending event to Google Analytics 4. timestamp_micros should be int64 or between 72 hours in the past and 15 minutes in the future.',
    94: 'Error in sending event to Google Analytics 4. non_personalized_ads should be bool.',
    95: 'Error in sending event to Google Analytics 4. user_id should be string.',
    96: 'Error in sending event to Google Analytics 4. user_properties contain invalid value.',
    97: 'Error in sending event to Google Analytics 4. events contain invalid value.',
    98: 'Error in sending event to Google Analytics 4. event params contain invalid value.',
    99: 'Error in sending event to Google Analytics 4. event params items contain invalid value.',
    100: 'Error in sending event to Google Analytics 4. payload is missing in event.',
    101: 'Error in sending event to Google Ads API. Bad format of Ads credential YAML.',
    102: 'Error in sending event to Ads Offline Conversion. Only 1 of gclid, gbraid, or wbraid should be set.',
})


# An enum with error names and error numbers to use for consistent logging and
# error handling across TCRM.
class ErrorNameIDMap(enum.Enum):
  """An enum maps error names and error numbers."""

  # Retriable error numbers start from 10
  ERROR = 10
  RETRIABLE_ERROR_OUTPUT_AUTHENTICATION_FAILED = 11
  RETRIABLE_ERROR_EVENT_NOT_SENT = 12
  RETRIABLE_ADS_HOOK_ERROR_UNAVAILABLE_ADS_SERVICE = 13
  RETRIABLE_ADS_HOOK_ERROR_FAIL_CREATING_USER_LIST = 14
  RETRIABLE_ADS_HOOK_ERROR_FAIL_ADDING_MEMBERS_TO_USER_LIST = 15
  RETRIABLE_ADS_UAC_HOOK_ERROR_FAIL_TO_GET_AIRFLOW_CONNECTION = 16
  RETRIABLE_ADS_UAC_HOOK_ERROR_MISSING_DEV_TOKEN = 17
  RETRIABLE_BQ_HOOK_ERROR_HTTP_ERROR = 18
  RETRIABLE_BQ_HOOK_ERROR_NO_TOTAL_ROWS = 19
  RETRIABLE_GA_HOOK_ERROR_HTTP_ERROR = 20
  RETRIABLE_GCS_HOOK_ERROR_HTTP_ERROR = 21
  RETRIABLE_GA4_HOOK_ERROR_HTTP_ERROR = 22
  RETRIABLE_CM360_HOOK_ERROR_HTTP_ERROR = 23
  RETRIABLE_DV360_HOOK_ERROR_HTTP_ERROR = 24

  # Non retriable error numbers start from 50
  NON_RETRIABLE_ERROR_EVENT_NOT_SENT = 50
  ADS_CM_HOOK_ERROR_PAYLOAD_FIELD_VIOLATES_SHA256_FORMAT = 51
  ADS_CM_HOOK_ERROR_EMAIL_VIOLATES_SHA256_FORMAT = 52
  ADS_CM_HOOK_ERROR_PHONE_NUMBER_VIOLATES_SHA256_FORMAT = 53
  ADS_CM_HOOK_ERROR_INVALID_EMAIL_AND_PHONE_NUMBER = 54
  ADS_CM_HOOK_ERROR_MISSING_USERID_IN_CRMID_EVENT = 55
  ADS_CM_HOOK_ERROR_MISSING_MOBILEID_IN_EVENT = 56
  ADS_CM_HOOK_ERROR_EMPTY_USER_LIST_NAME = 57
  ADS_CM_HOOK_ERROR_INVALID_MEMBERSHIP_LIFESPAN = 58
  ADS_CM_HOOK_ERROR_INVALID_UPLOAD_KEY_TYPE = 59
  ADS_CM_HOOK_ERROR_MISSING_APPID = 60
  ADS_CM_HOOK_ERROR_NOT_CREATING_LIST_WHEN_EMPTY_USER_LIST_NAME = 61
  ADS_HOOK_ERROR_FAIL_TO_GET_USER_LIST_ID = 62
  ADS_OC_HOOK_ERROR_MISSING_MANDATORY_FIELDS = 63
  ADS_OC_HOOK_ERROR_INVALID_LENGTH_OF_CONVERSION_NAME = 64
  ADS_OC_HOOK_ERROR_INVALID_FORMAT_OF_CONVERSION_TIME = 65
  ADS_OC_HOOK_ERROR_INVALID_CONVERSION_VALUE = 66
  ADS_OC_HOOK_ERROR_INVALID_LENGTH_OF_GOOGLE_CLICK_ID = 67
  ADS_UAC_HOOK_ERROR_MISSING_MANDATORY_FIELDS = 68
  ADS_UAC_HOOK_ERROR_UNSUPPORTED_APP_EVENT_TYPE = 69
  ADS_UAC_HOOK_ERROR_WRONG_APP_EVENT_TYPE = 70
  ADS_UAC_HOOK_ERROR_WRONG_RAW_DEVICE_ID_FORMAT = 71
  ADS_UAC_HOOK_ERROR_WRONG_RAW_DEVICE_ID_TYPE = 72
  ADS_UAC_HOOK_ERROR_WRONG_LAT_STATUS = 73
  BQ_HOOK_ERROR_NO_BLOBS = 74
  CM_HOOK_ERROR_MISSING_MANDATORY_FIELDS = 75
  CM_HOOK_ERROR_INVALID_VALUE_IN_CUSTOM_VARIABLES = 76
  CM_HOOK_ERROR_INVALID_CONVERSION_EVENT = 77
  GA_HOOK_ERROR_UNSUPPORTED_HIT_TYPE = 78
  GA_HOOK_ERROR_MISSING_CID_OR_UID = 79
  GA_HOOK_ERROR_BATCH_LENGTH_EXCEEDS_LIMITATION = 80
  GA_HOOK_ERROR_HIT_SIZE_EXCEEDS_LIMITATION = 81
  GA_HOOK_ERROR_INVALID_TRACKING_ID_FORMAT = 82
  GA_HOOK_ERROR_WRONG_SEND_TYPE = 83
  GCS_HOOK_ERROR_INVALID_BLOB_CONTENT_TYPE = 84
  GCS_HOOK_ERROR_MISSING_BLOB = 85
  GCS_HOOK_ERROR_BAD_JSON_FORMAT_BLOB = 86
  GCS_HOOK_ERROR_BAD_CSV_FORMAT_BLOB = 87
  GCS_HOOK_ERROR_DIFFERENT_ROW_LENGTH_IN_CSV_BLOB = 88
  MONITORING_HOOK_INVALID_VARIABLES = 89
  GA4_HOOK_ERROR_INVALID_JSON_STRUCTURE = 90
  GA4_HOOK_ERROR_INVALID_VALUES = 91
  GA4_HOOK_ERROR_VALUE_REQUIRED_CLIENT_ID = 92
  GA4_HOOK_ERROR_VALUE_INVALID_TIMESTAMP_MICROS = 93
  GA4_HOOK_ERROR_VALUE_INVALID_NON_PERSONALIZED_ADS = 94
  GA4_HOOK_ERROR_VALUE_INVALID_USER_ID = 95
  GA4_HOOK_ERROR_VALUE_INVALID_USER_PROPERTIES = 96
  GA4_HOOK_ERROR_VALUE_INVALID_EVENTS = 97
  GA4_HOOK_ERROR_VALUE_INVALID_EVENTS_PARAMS = 98
  GA4_HOOK_ERROR_VALUE_INVALID_EVENTS_PARAMS_ITEMS = 99
  GA4_HOOK_ERROR_MISSING_PAYLOAD_IN_EVENT = 100
  ADS_HOOK_ERROR_BAD_YAML_FORMAT = 101
  ADS_OC_HOOK_ERROR_INVALID_ID_CONFIG = 102
  ADS_SSI_HOOK_ERROR_MISSING_MANDATORY_FIELDS = 103


class Error(Exception):
  """Base error class for all Exceptions.

  Can store a custom message and a previous error, if exists, for more
  details and stack tracing use.
  """

  def __init__(
      self,
      msg: str = '',
      error_num: ErrorNameIDMap = ErrorNameIDMap.ERROR,
      error: Optional[Exception] = None,
  ) -> None:
    super(Error, self).__init__()
    self.error_num = error_num
    self.msg = msg
    self.prev_error = error

  def __repr__(self) -> str:
    reason = 'Error %d - %s' % (self.error_num.value, type(self).__name__)
    if self.msg:
      reason += ': %s' % self.msg
    if self.prev_error:
      reason += '\nSee causing error:\n%s' % str(self.prev_error)
    return reason

  __str__ = __repr__


# Airflow DAG related errors
class DAGError(Error):
  """Raised when there is an error creating a DAG."""


# Monitoring related errors
class MonitoringError(Error):
  """Raised when monitoring returns an error."""


class MonitoringValueError(MonitoringError):
  """Error occurred due to a wrong value being passed on."""


class MonitoringRunQueryError(MonitoringError):
  """Raised when querying monitoring DB returns an error."""


class MonitoringDatabaseError(MonitoringError):
  """Error occurred while Creating DB or table."""


class MonitoringAppendLogError(MonitoringError):
  """Error occurred while inserting log info into monitoring DB."""


class MonitoringCleanupError(MonitoringError):
  """Raised when an error occurs during monitoring table cleanup."""


# Data in connector related errors
class DataInConnectorError(Error):
  """Raised when an input data source connector returns an error."""


class DataInConnectorBlobParseError(DataInConnectorError):
  """Error occurred while parsing blob contents."""


class DataInConnectorValueError(DataInConnectorError):
  """Error occurred due to a wrong value being passed on."""


# Data out connector related errors
class DataOutConnectorError(Error):
  """Raised when an output data source connector returns an error."""


class DataOutConnectorValueError(DataOutConnectorError):
  """Error occurred due to a wrong value being passed on."""


class DataOutConnectorInvalidPayloadError(DataOutConnectorError):
  """Error occurred constructing or handling payload."""


class DataOutConnectorSendUnsuccessfulError(DataOutConnectorError):
  """Error occurred while sending data to data out source."""


class DataOutConnectorBlobReplacedError(DataOutConnectorError):
  """Error occurred while sending blob contents and Blob was replaced."""


class DataOutConnectorBlobProcessError(DataOutConnectorError):
  """Error occurred while sending some parts of blob contents."""


class DataOutConnectorAuthenticationError(DataOutConnectorError):
  """Error occurred while authenticating against the output resource."""
