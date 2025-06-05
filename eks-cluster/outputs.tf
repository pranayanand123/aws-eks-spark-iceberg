output "cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = module.eks.cluster_endpoint
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint for Hive Metastore"
  value       = aws_db_instance.hive_metastore.endpoint
}

output "iceberg_warehouse_bucket" {
  description = "S3 bucket for Iceberg warehouse"
  value       = aws_s3_bucket.iceberg_warehouse.bucket
}

output "input_data_bucket" {
  description = "S3 bucket for input log files"
  value       = aws_s3_bucket.input_data.bucket
}

output "processed_data_bucket" {
  description = "S3 bucket for processed data"
  value       = aws_s3_bucket.processed_data.bucket
}

output "spark_master_ui_url" {
  description = "Spark Master UI URL (after LoadBalancer is ready)"
  value       = "http://<EXTERNAL_IP>:8080 (Run 'kubectl get svc spark-master-svc' to get EXTERNAL_IP)"
}

output "kubectl_config_command" {
  description = "Command to configure kubectl"
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}

output "spark_master_pod_access" {
  description = "Command to access Spark master pod for development"
  value       = "kubectl exec -it $(kubectl get pods -l app=spark,role=master -o jsonpath='{.items[0].metadata.name}') -- /bin/bash"
}