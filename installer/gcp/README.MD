# Terraform

## Preparing for deployment

This installer is using [Terraform](https://www.terraform.io) to define all resources required to set up Tightlock on Google Cloud Platform.
Click the below button to clone this repository in Google Cloud and get started.

[![Open in Cloud Shell](https://gstatic.com/cloudssh/images/open-btn.svg)](https://shell.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2Fgoogle%2FTightlock&cloudshell_git_branch=main&cloudshell_open_in_editor=installer%2Fgcp%2Fvariables.tfvars)

### Update variables.tfvars file

Update the `variables.tfvars` file and provide the following values. If you clicked the `Open in Cloud Shell` button this file should be opened directly in your Cloud Shell Editor.

| variable                 | description                                                                                               |
| ---------------------    | --------------------------------------------------------------------------------------------------------- |
| api_key                  | API Key for generating the front end connection key.                                                      |
| project_id               | Google Cloud Project ID                                                      |
| create_bq_sample_data    | If set to true a BigQuery dataset and tables will be created with sample data uploaded via a Cloud Bucket |
| create_tightlock_network | If set to true this will create a VPC Network and Firewall rules used by the backend instance. Set it to false when deploying multiple Tightlock instances to the same cloud project. |
| compute_engine_zone      | Google Compute Engine Zone. Defaults to target project default if not provided.   |
| compute_engine_region    | Google Compute Engine Region. Defaults to target project default if not provided. |

## Deploy

To deploy Tightlock via Terraform run the below two commands in Cloud Shell Terminal.

The `init` command will initialise the Terraform configuration.

```shell
terraform init
```

The `apply` command will apply the resources to GCP, all required changes are listed when you run this command and you'll be prompted to confirm before anything is applied.

```shell
terraform apply -var-file=variables.tfvars
```

## Destroy deployed resources

In case you want to undo the deployment you can run the below command from the same directory as you've deployed it from.

```shell
terraform destroy -var-file=variables.tfvars
```
