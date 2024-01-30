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

The recommended way to configure connections in Tightlock is to link your backend instance in the 1PD Scheduler tool: https://1pd-scheduler.dev

In the tool:

1. Sign-in using a Gmail / Google Workspaces account
2. If you don't have a workspace yet, create a new one, choosing a name and providing the **connection code** that was generated during the deployment process. 
3. Once you have a workspace selected, all connections created in this workspace context will be using the backend instance tied to the connection code that was provided.
