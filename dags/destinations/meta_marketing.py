"""Meta Marketing destination implementation for Tightlock."""

import hashlib
import logging
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Sequence

import requests
from pydantic import Field

import errors
from utils import ProtocolSchema, RunResult, ValidationResult, TadauMixin, AdsPlatform, EventAction

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PayloadType(Enum):
    CREATE_USER = "CREATE_USER"
    UPDATE_USER = "UPDATE_USER"

class Destination(TadauMixin):
    """Implements DestinationProto protocol for Meta Marketing API."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__()
        self.config = config
        self.access_token = config["access_token"]
        self.ad_account_id = config["ad_account_id"]
        if not self.ad_account_id.startswith("act_"):
            self.ad_account_id = f"act_{self.ad_account_id}"
        self.payload_type = config["payload_type"]
        self.api_version = "v17.0"
        self.base_url = f"https://graph.facebook.com/{self.api_version}"
        self.audience_name = config.get("audience_name", "Tightlock CRM Audience")

        self._validate_credentials()

    def _validate_credentials(self) -> None:
        if not self.access_token or not self.ad_account_id:
            raise errors.DataOutConnectorValueError("Token d'accès et ID du compte publicitaire requis.")

    def _check_token_validity(self) -> bool:
        url = f"{self.base_url}/debug_token"
        params = {
            "input_token": self.access_token,
            "access_token": self.access_token
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json().get("data", {})
            if data.get("is_valid"):
                logger.info("Token d'accès valide.")
                return True
            else:
                logger.error(f"Token d'accès invalide: {data.get('error', {}).get('message')}")
        else:
            logger.error(f"Erreur lors de la vérification du token: {response.text}")
        return False

    def _check_ad_account_access(self) -> bool:
        url = f"{self.base_url}/{self.ad_account_id}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            logger.info(f"Accès au compte publicitaire confirmé: {response.json().get('name')}")
            return True
        else:
            logger.error(f"Erreur d'accès au compte publicitaire: {response.text}")
            return False

    def _build_api_url(self) -> str:
        return f"{self.base_url}/{self.ad_account_id}/customaudiences"

    def _hash_data(self, data: str) -> str:
        return hashlib.sha256(data.encode('utf-8')).hexdigest()
    
    def _format_payload(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "name": self.audience_name,
            "subtype": "CUSTOM",
            "description": "Audience créée à partir des données CRM",
            "customer_file_source": "USER_PROVIDED_ONLY",
            "schema": ["EMAIL", "PHONE", "FN"],
            "data": [
                [
                    self._hash_data(item.get("email", "")),
                    self._hash_data(item.get("phone", "")),
                    item.get("first_name", "")
                ] for item in data
            ]
        }

    def _send_payload(self, payload: Dict[str, Any]) -> requests.Response:
        url = self._build_api_url()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response

    def send_data(
        self, input_data: List[Mapping[str, Any]], dry_run: bool
    ) -> Optional[RunResult]:
        if not self._check_token_validity():
            logger.error("Token d'accès invalide. Veuillez générer un nouveau token.")
            return RunResult(successful_hits=0, failed_hits=len(input_data), error_messages=["Invalid access token"], dry_run=dry_run)

        if not self._check_ad_account_access():
            logger.error("Impossible d'accéder au compte publicitaire. Vérifiez l'ID du compte et les permissions.")
            return RunResult(successful_hits=0, failed_hits=len(input_data), error_messages=["Unable to access ad account"], dry_run=dry_run)

        try:
            payload = self._format_payload(input_data)
            if not dry_run:
                response = self._send_payload(payload)
                logger.info(f"Audience personnalisée créée/mise à jour avec succès: {response.json()}")
                run_result = RunResult(
                    successful_hits=len(input_data),
                    failed_hits=0,
                    error_messages=[],
                    dry_run=dry_run
                )
            else:
                logger.info(f"Dry-run: Données qui seraient envoyées: {payload}")
                run_result = RunResult(
                    successful_hits=len(input_data),
                    failed_hits=0,
                    error_messages=[],
                    dry_run=dry_run
                )

            # Collect usage data
            self.send_usage_event(
                ads_platform=AdsPlatform.META,
                event_action=EventAction.AUDIENCE,
                run_result=run_result,
                ads_platform_id=self.ad_account_id
            )

            return run_result

        except requests.HTTPError as e:
            logger.error(f"Erreur HTTP lors de la création/mise à jour de l'audience: {e}")
            logger.error(f"Réponse de l'API: {e.response.text}")
            return RunResult(successful_hits=0, failed_hits=len(input_data), error_messages=[str(e)], dry_run=dry_run)
        except requests.RequestException as e:
            logger.error(f"Erreur lors de la création/mise à jour de l'audience: {str(e)}")
            return RunResult(successful_hits=0, failed_hits=len(input_data), error_messages=[str(e)], dry_run=dry_run)

    @staticmethod
    def schema() -> Optional[ProtocolSchema]:
        return ProtocolSchema(
            "META_MARKETING",
            [
                ("access_token", str, Field(
                    description="Access token for Meta Marketing API")),
                ("ad_account_id", str, Field(
                    description="Ad account ID")),
                ("payload_type", PayloadType, Field(
                    description="Type of payload (CREATE_USER or UPDATE_USER)")),
                ("audience_name", Optional[str], Field(
                    default="Tightlock CRM Audience",
                    description="Name of the custom audience to create or update")),
            ]
        )

    def fields(self) -> Sequence[str]:
        return ["email", "phone", "first_name"]

    def batch_size(self) -> int:
        return 10000

    def validate(self) -> ValidationResult:
        if not self._check_token_validity():
            return ValidationResult(False, ["Invalid access token"])
        if not self._check_ad_account_access():
            return ValidationResult(False, ["Unable to access ad account"])
        return ValidationResult(True, [])

if __name__ == "__main__":
    config = {
        "access_token": "YOUR_ACCESS_TOKEN_HERE",
        "ad_account_id": "YOUR_AD_ACCOUNT_ID_HERE",
        "payload_type": PayloadType.CREATE_USER
    }

    destination = Destination(config)

    test_data = [
        {"email": "test1@example.com", "phone": "1234567890", "first_name": "John"},
        {"email": "test2@example.com", "phone": "0987654321", "first_name": "Jane"}
    ]

    result = destination.send_data(test_data, dry_run=False)
    print(result)