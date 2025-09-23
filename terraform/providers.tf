terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    # Databricks provider for managing workspaces and other resources
    databricks = {
      source  = "databricks/databricks"
      version = "1.27.0"
    }
  }
}

# Configure the AWS provider
provider "aws" {
  region = "eu-north-1" 
}

# Configure the Databricks provider
# For CI/CD, host and token should be set as environment variables in GitHub Actions secrets
# DATABRICKS_HOST and DATABRICKS_TOKEN
provider "databricks" {}
