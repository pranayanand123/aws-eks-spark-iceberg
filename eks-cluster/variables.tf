variable "region" {
  description = "AWS region"
  type        = string
  default     = "ap-south-1"
}

variable "cluster_name" {
  description = "EKS cluster name"
  type        = string
  default     = "spark-eks-cluster"
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.0.0.0/16"
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
  default     = "my-unique-iceberg-warehouse-bucket"
}

# Database password for RDS
variable "db_password" {
  description = "Password for the Hive Metastore database"
  type        = string
  sensitive   = true
  default     = "HiveMetastore123!"
}

# Additional variables for the complete setup
variable "enable_spot_instances" {
  description = "Enable spot instances for worker nodes"
  type        = bool
  default     = true
}

variable "worker_instance_types" {
  description = "Instance types for worker nodes"
  type        = list(string)
  default     = ["t3.medium", "t3.large"]
}

variable "master_instance_types" {
  description = "Instance types for master nodes"
  type        = list(string)
  default     = ["t3.medium"]
}