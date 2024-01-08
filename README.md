Copyright Google LLC. Supported by Google LLC and/or its affiliate(s). This solution, including any related sample code or data, is made available on an “as is,” “as available,” and “with all faults” basis, solely for illustrative purposes, and without warranty or representation of any kind. This solution is experimental, unsupported and provided solely for your convenience. Your use of it is subject to your agreements with Google, as applicable, and may constitute a beta feature as defined under those agreements. To the extent that you make any data available to Google in connection with your use of the solution, you represent and warrant that you have all necessary and appropriate rights, consents and permissions to permit Google to use and process that data. By using any portion of this solution, you acknowledge, assume and accept all risks, known and unknown, associated with its usage and any processing of data by Google, including with respect to your deployment of any portion of this solution in your systems, or usage in connection with your business, if at all. With respect to the entrustment of personal information to Google, you will verify that the established system is sufficient by checking Google's privacy policy and other public information, and you agree that no further information will be provided by Google.

# Tightlock

Tightlock is an open source tool, that can be used to transport first-party data to Google ads platforms (e.g., Ads, Google Marketing Platform) via Google APIs.

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

Tightlock runs on Docker, so it can be deployed virtually anywhere. You can find instructions below on running on local machines and deploying in GCP with Terraform. Similar Terraform installers for AWS and Azure are coming soon.

### Running locally
See [Developer Workflow](https://github.com/google-marketing-solutions/Tightlock/wiki/4.-Developing-new-connections) for detailed instructions for running Tightlock locally for development.

### GCP Deploy

This installer is using [Terraform](https://www.terraform.io) to define all resources required to set up Tightlock on Google Cloud Platform.
Click the below button to clone this repository in Google Cloud and get started.

[![Open in Cloud Shell](https://gstatic.com/cloudssh/images/open-btn.svg)](https://shell.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2Fgoogle%2FTightlock&cloudshell_git_branch=main&cloudshell_open_in_editor=installer%2Fvariables.tfvars)

#### **Update variables.tfvars file**

Update the `variables.tfvars` file and provide the following two values. If you clicked the `Open in Cloud Shell` button this file should be opened directly in your Cloud Shell Editor.

| variable                 | description                                                                                               |
| ---------------------    | --------------------------------------------------------------------------------------------------------- |
| api_key                  | API Key for generating the front end connection key.                                                      |
| project_id               | Google Cloud Project ID                                                                                   |
| create_bq_sample_data    | If set to true a BigQuery dataset and tables will be created with sample data uploaded via a Cloud Bucket |
| create_tightlock_network | If set to true this will create a VPC Network and Firewall rules used by the backend instance. Set it to false when deploying multiple Tightlock instances to the same cloud project. |

#### **Deploy**

To deploy Tightlock via Terraform run the below two commands in Cloud Shell Terminal.

The `init` command will initialise the Terraform configuration.

```shell
terraform init
```

The `apply` command will apply the resources to GCP, all required changes are listed when you run this command and you'll be prompted to confirm before anything is applied.

```shell
terraform apply -var-file=variables.tfvars
```

#### **Destroy deployed resources**

In case you want to undo the deployment you can run the below command from the same directory as you've deployed it from.

```shell
terraform destroy -var-file=variables.tfvars
```

### AWS Deploy

> **COMING SOON**

### Azure Deploy

> **COMING SOON**
