# 1. Configure the AWS Provider
# This tells Terraform you are working with AWS and in which region.
provider "aws" {
  region = "us-east-1" # You can change this to your preferred AWS region
}

# 2. Create the S3 Bucket
# This resource defines the S3 bucket that will serve as your data lake.
resource "aws_s3_bucket" "data_lake" {
  # IMPORTANT: S3 bucket names must be globally unique.
  # Change this to a unique name, e.g., "yourname-wartsila-showcase-2025"
  bucket = "your-unique-datalake-bucket-name"
  acl    = "private"

  tags = {
    Name        = "Wartsila-Showcase-Data-Lake"
    Project     = "Data Engineer Interview"
    ManagedBy   = "Terraform"
  }
}

# 3. Create the IAM Policy for S3 Access
# This policy document specifies exactly what actions the IAM role will be allowed to perform.
resource "aws_iam_policy" "databricks_s3_access_policy" {
  name        = "databricks-s3-access-policy"
  description = "Allows Databricks to access the specific S3 data lake bucket."

  # The policy itself, written in JSON.
  # It grants permissions to read, write, delete, and list objects in the bucket.
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ],
        Resource = [
          aws_s3_bucket.data_lake.arn,       # Grants access to the bucket itself
          "${aws_s3_bucket.data_lake.arn}/*" # Grants access to the objects inside the bucket
        ]
      }
    ]
  })
}

# 4. Create the IAM Role for Databricks
# This creates the IAM role that your Databricks workspace will assume.
resource "aws_iam_role" "databricks_role" {
  name = "databricks-cross-account-role"

  # This is the trust policy. It defines WHO can assume this role.
  # IMPORTANT: The ARN here is a placeholder. You will need to find the correct
  # ARN for your specific Databricks setup. For now, this placeholder is sufficient
  # to create the role.
  assume_role_policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = {
          # This ARN allows a Databricks service to assume the role.
          # You would get the correct value from your Databricks account settings
          # when creating a cluster or storage credential.
          AWS = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCAzureMainRole-1AJ6UQSSB8F0Q"
        },
        Action    = "sts:AssumeRole"
      },
    ]
  })
}

# 5. Attach the Policy to the Role
# This final step connects the policy (the permissions) to the role (the identity).
resource "aws_iam_role_policy_attachment" "attach_s3_policy_to_role" {
  role       = aws_iam_role.databricks_role.name
  policy_arn = aws_iam_policy.databricks_s3_access_policy.arn
}

