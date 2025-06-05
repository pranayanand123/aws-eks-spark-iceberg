# S3 bucket for Iceberg warehouse
resource "aws_s3_bucket" "iceberg_warehouse" {
  bucket = var.iceberg_warehouse_bucket
}

resource "aws_s3_bucket_versioning" "iceberg_warehouse" {
  bucket = aws_s3_bucket.iceberg_warehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "iceberg_warehouse" {
  bucket = aws_s3_bucket.iceberg_warehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# S3 bucket for input data (log files)
resource "aws_s3_bucket" "input_data" {
  bucket = "${var.cluster_name}-input-data"
}

resource "aws_s3_bucket_versioning" "input_data" {
  bucket = aws_s3_bucket.input_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "input_data" {
  bucket = aws_s3_bucket.input_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# S3 bucket for processed data and application JARs
resource "aws_s3_bucket" "processed_data" {
  bucket = "${var.cluster_name}-processed-data"
}

resource "aws_s3_bucket_versioning" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# IAM policy for S3 access
resource "aws_iam_policy" "s3_access" {
  name        = "spark-s3-access"
  description = "IAM policy for Spark S3 access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload",
          "s3:GetObjectVersion",
          "s3:PutObjectAcl"
        ]
        Resource = [
          aws_s3_bucket.iceberg_warehouse.arn,
          "${aws_s3_bucket.iceberg_warehouse.arn}/*",
          aws_s3_bucket.input_data.arn,
          "${aws_s3_bucket.input_data.arn}/*",
          aws_s3_bucket.processed_data.arn,
          "${aws_s3_bucket.processed_data.arn}/*"
        ]
      }
    ]
  })
}
