import requests
import json
import os
from enum import Enum

# Use environment variables for configuration
TIGHTLOCK_IP = os.getenv('TIGHTLOCK_IP', '{ADDRESS}')
API_KEY = os.getenv('TIGHTLOCK_API_KEY', '{EXAMPLE_API_KEY}')

BASE_URL = f"http://{TIGHTLOCK_IP}/api/v1"

headers = {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY
}

class PayloadType(Enum):
    CREATE_USER = "CREATE_USER"
    UPDATE_USER = "UPDATE_USER"

def create_new_config(config_data):
    """
    Create a new configuration in Tightlock.
    
    :param config_data: dict containing the configuration data
    :return: Response from the API
    """
    url = f"{BASE_URL}/configs"
    response = requests.post(url, headers=headers, json=config_data)
    return response.json()

def get_current_config():
    """
    Get the current configuration from Tightlock.
    
    :return: Current configuration in JSON format
    """
    url = f"{BASE_URL}/configs:getLatest"
    response = requests.get(url, headers=headers)
    return response.json()

def trigger_connection(connection_name, dry_run=False):
    """
    Trigger an existing connection in Tightlock.
    
    :param connection_name: Name of the connection to trigger
    :param dry_run: Boolean indicating whether to perform a dry run
    :return: Response from the API
    """
    url = f"{BASE_URL}/activations/{connection_name}:trigger"
    data = {"dry_run": 1 if dry_run else 0}
    response = requests.post(url, headers=headers, json=data)
    return response.text

def test_connection():
    """
    Test the connection to the Tightlock API.
    
    :return: Response from the API
    """
    url = f"{BASE_URL}/connect"
    response = requests.post(url, headers=headers)
    return response.text, response.status_code

def setup():
    # Example configuration with Meta Marketing destination
    new_config = {
        "label": "TEST 3 BQ to meta",
        "value": {
            "external_connections": [],

            "sources": {
                "test3_bq": {
                    "type": "BIGQUERY",
                    "dataset": "tightlock_sample_data",
                    "table": "test_meta_table",
                    "unique_id" : "email"
                }
            },

            "destinations": {
                "test3_meta": {
                    "type": "META_MARKETING",
                    "access_token": "EAAL9plm0REcBOxZAoHFHMeFCz3bNZCZCravyapbkuFKymTvFenk46JXWZAOxsZAZAZBrhd56cKub7p924ZAEss7RXwc3gAYgWAa6cA7PL9jCSFZBzbvOosPHC4kjVZB4fnxQWyeozblxZC7h05ZCZAlgX0eVUYWrf9XJhGq5TQ756IrpomzqrEHHoC0F85MJL7VZBQkXSLFCBYyOYZBXP0EntKjJSL5YhOU3TkZD",
                    "ad_account_id": "1158906525188725",
                    "payload_type": PayloadType.CREATE_USER.value,
                    "audience_name": "Marketing TL Audience from BQ 2"
                }   
            },

            "activations": [
                {
                    "name": "test_3_bq_to_meta",
                    "source": {
                        "$ref": "#/sources/test3_bq"
                    },
                    "destination": {
                        "$ref": "#/destinations/test3_meta"
                    },
                    "schedule": "@hourly"
                }
            ],

            "secrets": {},
        }
    }
    
    # Create new config
    print("Creating new config:")
    print(create_new_config(new_config))

    # Get current config
    print("\nGetting current config:")
    print(get_current_config())


# Example usage:
if __name__ == "__main__":
    # Test the connection
    response, status_code = test_connection()
    print(f"Connection test response (status {status_code}): {response}")
    
    #setup()

    # Trigger the connection (dry run)
    print("\nTriggering connection (dry run):")
    print(trigger_connection("test_3_bq_to_meta", dry_run=True))