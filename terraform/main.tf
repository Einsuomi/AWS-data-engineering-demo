# Dynamically create one S3 bucket for each Medallion layer in the current environment
resource "aws_s3_bucket" "medallion_buckets" {
  # for_each creates a resource for each item in the var.medallion_layers list
  for_each = toset(var.medallion_layers)

  # The bucket name is dynamically constructed using the environment and layer name
  bucket = "wartsila-datalake-${var.environment}-${each.key}"
  acl    = "private"

  tags = {
    Name        = "DataLake-${var.environment}-${each.key}"
    Environment = var.environment
    Layer       = each.key
    ManagedBy   = "Terraform"
  }
}

# --- Placeholder for Databricks Workspace Creation ---

/*
resource "databricks_mws_workspaces" "this" {
  provider       = databricks.mws
  account_id     = var.databricks_account_id # Needs your Databricks Account ID
  workspace_name = "dbw-w-${var.environment}-ne-001"
  aws_region     = "us-east-1"

  # Requires pre-configured network and credentials ARNs
  credentials_id = var.databricks_credentials_id
  storage_configuration_id = var.databricks_storage_configuration_id
  network_id     = var.databricks_network_id
}
*/
