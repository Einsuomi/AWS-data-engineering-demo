terraform {
  backend "s3" {
    # The name of the S3 bucket you just created
    bucket = "w-tfstate-001"

    # A path inside the bucket to store the state file for this project
    # Using workspaces here ensures dev, test, and prod have separate state files
    key    = "wartsila-showcase/terraform.tfstate"
    
    # The region your S3 bucket is in
    region = "eu-north-1"

    # The name of the DynamoDB table you just created
    dynamodb_table = "terraform-state-locks"
    
    # Encrypts your state file at rest
    encrypt        = true
  }
}
