>Copyright Google LLC. Supported by Google LLC and/or its affiliate(s). This solution, including any related sample code or data, is made available on an “as is,” “as available,” and “with all faults” basis, solely for illustrative purposes, and without warranty or representation of any kind. This solution is experimental, unsupported and provided solely for your convenience. Your use of it is subject to your agreements with Google, as applicable, and may constitute a beta feature as defined under those agreements. To the extent that you make any data available to Google in connection with your use of the solution, you represent and warrant that you have all necessary and appropriate rights, consents and permissions to permit Google to use and process that data. By using any portion of this solution, you acknowledge, assume and accept all risks, known and unknown, associated with its usage and any processing of data by Google, including with respect to your deployment of any portion of this solution in your systems, or usage in connection with your business, if at all. With respect to the entrustment of personal information to Google, you will verify that the established system is sufficient by checking Google's privacy policy and other public information, and you agree that no further information will be provided by Google.


# Tightlock - First-Party Data Tool

Named after the automatic joining mechanism used between train cars, *Tightlock* is an open source tool that can be used to transport first-party data to Google ads platforms (e.g., Ads, Google Marketing Platform) via Google APIs.

Some examples of connections that are available through Tightlock are:

- Google Analytics / GA4 measurement 
- Google Ads API Customer Match
- Campaign Manager 360 Offline Conversion Import
- Google Ads Offline Conversion Import
- Google Ads Enhanced Conversions for Leads
- Google Ads Enhanced Conversions for Web
- Google Ads Conversion Adjustments
- Google Ads Store Sales Improvements
- Display Video 360 Customer Match

Tightlock serves as a cloud-agnostic pipeline tool to pull data from a variety of customer sources (e.g., BigQuery, Cloud Storage, S3 etc) for transfer into Google APIs.

You can learn more about Tightlock by watching this [introduction video](https://www.youtube.com/watch?v=8Y5HZIZXyzo).

## Installation

Tightlock runs on Docker, so it can be deployed virtually anywhere. You can find instructions below on running on local machines and deploying in GCP or AWS with Terraform. Similar Terraform installer for Azure is coming soon.



### Running locally
See [Developer Workflow](https://github.com/google-marketing-solutions/Tightlock/wiki/4.-Developing-new-connections) for detailed instructions for running Tightlock locally for development.

### GCP Deploy

You can click the button below to start deployment of Tightlock in GCP. You can find detailed instructions for GCP deployment [here](https://github.com/google-marketing-solutions/Tightlock/tree/main/installer/gcp).

[![Open in Cloud Shell](https://gstatic.com/cloudssh/images/open-btn.svg)](https://shell.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2Fgoogle%2FTightlock&cloudshell_git_branch=main&cloudshell_open_in_editor=installer%2Fvariables.tfvars)


### AWS Deploy

For deploying in AWS, follow instructions [here](https://github.com/google-marketing-solutions/Tightlock/tree/main/installer/aws).

### Azure Deploy

> **COMING SOON**

## General usage

Below, you will find simplified instructions for using Tightlock. For a more detailed guide, refer to [this presentation](https://services.google.com/fh/files/misc/hack_a_tech_june_tightlock__.pdf).

### Workspace creation
The recommended way to configure connections in Tightlock is to link your backend instance in the 1PD Scheduler tool: https://1pd-scheduler.dev

In the tool:

1. Sign-in using a Gmail / Google Workspaces account
2. If you don't have a workspace yet, create a new one, choosing a name and providing the **connection code** that was generated during the deployment process. 
3. Once you have a workspace selected, all connections created in this workspace context will be using the backend instance tied to the connection code that was provided.

### Creating connections

Connections are the core concept of Tightlock. A connection is defined by a source of data, a destination (tipically, a Google API) and a schedule (or None).

You can create new connections by:

- Directly using the [1PD Scheduler](https://1pd-scheduler.dev) tool. Visit our Wiki for a detailed specification of [Sources](https://github.com/google-marketing-solutions/Tightlock/wiki/2.-Source-Specification) and [Destinations](https://github.com/google-marketing-solutions/Tightlock/wiki/1.-Destination-Specification)

- Using the [Tightlock API](#tightlock-api), described below.


## Tightlock API

Tightlock communicates with https://1pd-scheduler.dev by using a REST API. This API can also be directly accessed by customers that are not interested in configuring the backend using the UI.

You can find a quick summary of the main actions that are available in the API.

> Note: Bear in mind that the default deployment of Tightlock has a security measure of limiting the IPs that can call the API. If you want to use the API direclty, make sure to change this configuration or call the API using an internal IP address.

<br>

### Create a new config

<details>
 <summary><code>POST </code>/api/v1/configs<code></code> </summary>

#### **Payload**

example.json file:

```json
{
  "label": "Example BQ to GA4 App",
  "value": {
    "external_connections": [], 

    "sources": {
      "example_bigquery_table": {
        "type": "BIGQUERY",
        "dataset": "bq_dataset_example_name",
        "table": "bq_table_example_name"
      }
    },

    "destinations": {
      "example_ga4_app": {
        "type": "GA4MP",
        "payload_type": "firebase",
        "api_secret": "fake_api_secret",
        "firebase_app_id": "fake_firebase_app_id"
      }   
    },

    "activations": [
      {
        "name": "example_bq_to_ga4mp_app_event",
        "source": {
          "$ref": "#/sources/example_bigquery_table"
        },
        "destination": {
          "$ref": "#/destinations/example_ga4_app"
        },
        "schedule": "@weekly"
      }
    ], 

    "secrets": {},
  }
}
```

Bear in mind that "label" must be unique.

#### **Responses**

> | http code     | content-type                      | response                                                            |
> |---------------|-----------------------------------|---------------------------------------------------------------------|
> | `200`         | `application/json`        | `Configuration created successfully`                                |
> | `409`         | `application/json`                | `{"code":"409","message":"Config label already exists"}`                            |

#### **Example cURL**

> ```javascript
>  curl -H "Content-Type: application/json" -X POST -H 'X-Api-Key: {EXAMPLE_API_KEY}'  {ADDRESS}:8081/api/v1/configs -d @example.json
> ```

</details>

<br>

### Get the current config

<details>
 <summary><code>GET</code>/api/v1/configs:getLatest<code></code> </summary>

#### **Payload**

None

#### **Responses**

> | http code     | content-type                      | response                                                            |
> |---------------|-----------------------------------|---------------------------------------------------------------------|
> | `200`         | `application/json`        | Config in JSON format                                |                    |

#### **Example cURL**

> ```javascript
>  curl -H "Content-Type: application/json" -H 'X-Api-Key: {EXAMPLE_API_KEY}' {ADDRESS}:8081/api/v1/configs:getLatest                      
> ```

</details>

<br>

### Trigger an existing connection


<details>
 <summary><code>GET</code>/api/v1/connection:{connection_name}<code></code> </summary>

#### **Payload**

> | name      |  type     | data type               | description                                                           |
> |-----------|-----------|-------------------------|-----------------------------------------------------------------------|
> | connection_name |  required | str   | Target connection
> | dry_run |  not required | int   | Whether or not to do a dry-run for the target connection (0 is false and 1 is true)

#### **Responses**

> | http code     | content-type                      | response                                                            |
> |---------------|-----------------------------------|---------------------------------------------------------------------|
> | `200`         | `application/json`        | Trigger successful                                |                    |

#### **Example cURL**

> ```javascript
>  curl -X POST -H 'X-API-Key: {EXAMPLE_API_KEY}' -H 'Content-Type: application/json' -d '{"dry_run": 0}' -o - -i {ADDRESS}:8081/api/v1/activations/activation_name:trigger
> ```

</details>
