"""Google Ads Marketing destination implementation for Tightlock."""

import hashlib
import logging
import string
import uuid
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Sequence

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from pydantic import Field

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils import ProtocolSchema, RunResult, ValidationResult

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PayloadType(Enum):
    """Types of operations supported by the destination."""
    CREATE_AUDIENCE = "CREATE_AUDIENCE"
    UPDATE_AUDIENCE = "UPDATE_AUDIENCE"
    DELETE_AUDIENCE = "DELETE_AUDIENCE"

class Destination:
    """Implements DestinationProto protocol for Google Ads Marketing API."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize Google Ads Marketing destination."""
        # Store the configuration and customer ID
        self.config = config
        self.customer_id = str(config["customer_id"])
        
        # Create configuration dictionary for GoogleAdsClient
        client_config = {
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "developer_token": config["developer_token"],
            "refresh_token": config["refresh_token"],
            "login_customer_id": str(config.get("login_customer_id", config["customer_id"])),
            "use_proto_plus": True
        }
        
        # Initialize the Google Ads client
        self.client = GoogleAdsClient.load_from_dict(client_config)
        # Validate the credentials immediately
        self._validate_credentials()
        
        self.payload_type = config["payload_type"]
        self.audience_name = config.get("audience_name", f"Customer Match list #{uuid.uuid4()}")

    def _validate_credentials(self) -> None:
        """Validate Google Ads credentials."""
        try:
            self.client.get_service("UserListService")
        except GoogleAdsException as e:
            raise ValueError(f"Invalid Google Ads credentials: {str(e)}")

    @staticmethod
    def normalize_and_hash(s: str, remove_punctuation: bool = False) -> str:
        """Normalize and hash a string value according to Google Ads requirements."""
        if not s:
            return ""
        
        # Convert to lowercase and remove leading/trailing whitespace
        s = s.strip().lower()
        
        # Remove punctuation if requested
        if remove_punctuation:
            s = s.translate(str.maketrans("", "", string.punctuation))
            
        # Return SHA256 hash of the normalized string
        return hashlib.sha256(s.encode()).hexdigest()

    def _create_customer_match_user_list(self) -> Optional[str]:
        """Create a new Customer Match user list."""
        user_list_service = self.client.get_service("UserListService")
        user_list_operation = self.client.get_type("UserListOperation")
        
        # Configure the user list settings
        user_list = user_list_operation.create
        user_list.name = self.audience_name
        user_list.description = "A list of customers from email and physical addresses"
        user_list.crm_based_user_list.upload_key_type = (
            self.client.enums.CustomerMatchUploadKeyTypeEnum.CONTACT_INFO
        )
        user_list.membership_life_span = 30
        
        try:
            response = user_list_service.mutate_user_lists(
                customer_id=self.customer_id,
                operations=[user_list_operation]
            )
            user_list_resource_name = response.results[0].resource_name
            logger.info(f"Created user list: {user_list_resource_name}")
            return user_list_resource_name
        except GoogleAdsException as e:
            self._handle_googleads_exception(e)
            return None

    def _build_offline_user_data_job_operations(self, records: List[Dict[str, Any]]) -> List:
        """Build offline user data job operations from customer records."""
        operations = []
        for record in records:
            user_data = self.client.get_type("UserData")
            
            # Add email if provided
            if "email" in record:
                user_identifier = self.client.get_type("UserIdentifier")
                user_identifier.hashed_email = self.normalize_and_hash(record["email"], True)
                user_data.user_identifiers.append(user_identifier)
                
            # Add phone if provided
            if "phone" in record:
                user_identifier = self.client.get_type("UserIdentifier")
                user_identifier.hashed_phone_number = self.normalize_and_hash(record["phone"], True)
                user_data.user_identifiers.append(user_identifier)
                
            # Add address info if all required fields are provided
            if all(key in record for key in ("first_name", "last_name", "country_code", "postal_code")):
                user_identifier = self.client.get_type("UserIdentifier")
                address_info = user_identifier.address_info
                address_info.hashed_first_name = self.normalize_and_hash(record["first_name"], False)
                address_info.hashed_last_name = self.normalize_and_hash(record["last_name"], False)
                address_info.country_code = record["country_code"]
                address_info.postal_code = record["postal_code"]
                user_data.user_identifiers.append(user_identifier)
                
            if user_data.user_identifiers:
                operation = self.client.get_type("OfflineUserDataJobOperation")
                operation.create = user_data
                operations.append(operation)
                
        return operations

    def _add_users_to_customer_match_list(self, user_list_resource_name: str, records: List[Dict[str, Any]]) -> bool:
        """Add users to an existing customer match user list."""
        try:
            # Get the OfflineUserDataJobService
            offline_user_data_job_service = self.client.get_service("OfflineUserDataJobService")
            
            # Create a new offline user data job
            job = self.client.get_type("OfflineUserDataJob")
            job.type_ = self.client.enums.OfflineUserDataJobTypeEnum.CUSTOMER_MATCH_USER_LIST
            job.customer_match_user_list_metadata.user_list = user_list_resource_name
            
            # Create the job
            create_response = offline_user_data_job_service.create_offline_user_data_job(
                customer_id=self.customer_id,
                job=job
            )
            
            job_resource_name = create_response.resource_name
            logger.info(f"Created offline user data job: {job_resource_name}")
            
            # Build and add the operations
            operations = self._build_offline_user_data_job_operations(records)
            request = self.client.get_type("AddOfflineUserDataJobOperationsRequest")
            request.resource_name = job_resource_name
            request.operations = operations
            
            # Add the operations to the job
            response = offline_user_data_job_service.add_offline_user_data_job_operations(
                request=request
            )
            logger.info(f"Added {len(operations)} operations to the job")
            
            # Run the job
            offline_user_data_job_service.run_offline_user_data_job(
                resource_name=job_resource_name
            )
            logger.info("Started the offline user data job")
            
            return True
            
        except GoogleAdsException as e:
            self._handle_googleads_exception(e)
            return False

    def _handle_googleads_exception(self, exception: GoogleAdsException) -> None:
        """Handle Google Ads API exceptions with detailed error reporting."""
        logger.error(f"Request ID '{exception.request_id}' failed with status "
                    f"'{exception.error.code().name}' and includes the following errors:")
        for error in exception.failure.errors:
            logger.error(f"\tError with message '{error.message}'.")
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    logger.error(f"\t\tOn field: {field_path_element.field_name}")

    def send_data(
        self, input_data: List[Mapping[str, Any]], dry_run: bool
    ) -> Optional[RunResult]:
        """Send data to Google Ads Marketing API."""
        try:
            if dry_run:
                return RunResult(
                    successful_hits=len(input_data),
                    failed_hits=0,
                    error_messages=[],
                    dry_run=True
                )

            # Create user list
            user_list_resource_name = self._create_customer_match_user_list()
            if not user_list_resource_name:
                return RunResult(
                    successful_hits=0,
                    failed_hits=len(input_data),
                    error_messages=["Failed to create user list"],
                    dry_run=False
                )

            # Add users to the list
            success = self._add_users_to_customer_match_list(user_list_resource_name, input_data)
            if success:
                return RunResult(
                    successful_hits=len(input_data),
                    failed_hits=0,
                    error_messages=[],
                    dry_run=False
                )
            else:
                return RunResult(
                    successful_hits=0,
                    failed_hits=len(input_data),
                    error_messages=["Failed to add users to list"],
                    dry_run=False
                )

        except Exception as e:
            error_message = f"Error sending data: {str(e)}"
            logger.error(error_message)
            return RunResult(
                successful_hits=0,
                failed_hits=len(input_data),
                error_messages=[error_message],
                dry_run=dry_run
            )

    @staticmethod
    def schema() -> Optional[ProtocolSchema]:
        """Define the configuration schema."""
        return ProtocolSchema(
            "GOOGLE_ADS_MARKETING",
            [
                ("client_id", str, Field(
                    description="Google Ads API client ID")),
                ("client_secret", str, Field(
                    description="Google Ads API client secret")),
                ("developer_token", str, Field(
                    description="Google Ads API developer token")),
                ("refresh_token", str, Field(
                    description="OAuth2 refresh token")),
                ("customer_id", str, Field(
                    description="Google Ads customer ID")),
                ("login_customer_id", Optional[str], Field(
                    description="Google Ads login customer ID")),
                ("payload_type", PayloadType, Field(
                    description="Operation type (CREATE_AUDIENCE/UPDATE_AUDIENCE)")),
                ("audience_name", Optional[str], Field(
                    default="Tightlock CRM Audience",
                    description="Custom audience name"))
            ]
        )

    def fields(self) -> Sequence[str]:
        """Define required input fields."""
        return ["email", "phone", "first_name", "last_name", "country_code", "postal_code"]

    def batch_size(self) -> int:
        """Define maximum batch size."""
        return 10000

    def validate(self) -> ValidationResult:
        """Validate the configuration."""
        try:
            self._validate_credentials()
            return ValidationResult(True, [])
        except ValueError as e:
            return ValidationResult(False, [str(e)])

if __name__ == "__main__":
    # Example usage
    config = {
        "client_id": "",
        "client_secret": "",
        "developer_token": "",
        "refresh_token": "",
        "login_customer_id": "",
        "customer_id": "",
        "audience_name": "TEST CUST 6",
        "payload_type": PayloadType.CREATE_AUDIENCE
    }
    
    destination = Destination(config)
    
    test_data = [
        {
            "email": "test1@example.com",
            "phone": "+1 800 5550101"
        },
        {
            "email": "test2@example.com",
            "first_name": "John",
            "last_name": "Doe",
            "country_code": "US",
            "postal_code": "94045",
            "phone": "+1 800 5550102",
        },
        {
            "email": "test3@example.com"
        }
    ]
    
    result = destination.send_data(test_data, dry_run=False)
    print(result)