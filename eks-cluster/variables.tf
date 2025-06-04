variable "region" {
  default = "ap-south-1"
}

variable "cluster_name" {
  default = "spark-eks-cluster"
}

variable "vpc_cidr" {
  default = "10.0.0.0/16"
}

# AWS credentials for S3 access
variable "aws_access_key_id" {
  description = "AWS Access Key ID for S3 access"
  type        = string
  sensitive   = true
}

variable "aws_secret_access_key" {
  description = "AWS Secret Access Key for S3 access"
  type        = string
  sensitive   = true
}

variable "iceberg_warehouse_bucket" {
  description = "S3 bucket name for Iceberg warehouse"
  type        = string
  default     = "your-iceberg-warehouse-bucket"
}

# Database password for RDS
variable "db_password" {
  description = "Password for the Hive Metastore database"
  type        = string
  sensitive   = true
  default     = "your-hive-metastore-database-password"
}