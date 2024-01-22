
# Terraform

## Preparing for deployment

This installer is using [Terraform](https://www.terraform.io) to define all resources required to set up Tightlock on Amazon Web Services.


# TODO: Instructions to clone repo and modify variables

### Update variables.tfvars file

Update the `variables.tfvars` file and provide the following two values. If you clicked the `Open in Cloud Shell` button this file should be opened directly in your Cloud Shell Editor.

| variable                 | description                                                                                               |
| ---------------------    | --------------------------------------------------------------------------------------------------------- |
| api_key                  | API Key for generating the front end connection key.                                                      |
| access_key               | AWS access key. More details [here](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html) |
| secret_key               | AWS secret key. More details [here](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html) |


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
