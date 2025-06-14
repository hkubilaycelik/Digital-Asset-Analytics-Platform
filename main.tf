# Configures the AWS provider to use the us-east-1 region
provider "aws" {
    
  region = "us-east-1"
}

# Define S3 bucket
resource "aws_s3_bucket" "data_lake" {

  bucket = "digital-asset-analytics-data-lake" 

  tags = {
    Name      = "Data Lake"
    Project   = "Digital-Asset-Analytics-Platform"
    ManagedBy = "Terraform"
  }
}