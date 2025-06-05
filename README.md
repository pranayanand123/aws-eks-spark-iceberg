# AWS EKS Spark Iceberg Data Pipeline

A comprehensive log processing data pipeline that uses Apache Spark on Amazon EKS to process nginx logs, store them in Apache Iceberg format using Apache Hive Metastore, with data stored in Amazon S3. The entire infrastructure is deployed using Terraform.

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Running the Pipeline](#running-the-pipeline)
- [Querying Data](#querying-data)
- [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)
- [Assumptions and Design Decisions](#assumptions-and-design-decisions)

## 🏗️ Architecture Overview

This data pipeline processes nginx logs through the following stages:
1. **Data Generation**: Python script generates realistic nginx logs
2. **Data Storage**: Logs stored in S3 with date partitioning
3. **Data Processing**: Spark on EKS processes and enriches log data
4. **Data Lake**: Iceberg tables provide ACID transactions and schema evolution
5. **Analytics**: Daily and weekly analytics tables for insights

For detailed architecture, see [architecture-diagram.md](architecture-diagram.md).

## 📚 Prerequisites

### Required Tools
- **AWS CLI v2**: [Installation Guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
- **Terraform v1.0+**: [Installation Guide](https://learn.hashicorp.com/tutorials/terraform/install-cli)
- **kubectl**: [Installation Guide](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
- **Python 3.8+**: With pip package manager
- **Docker** (optional): For building custom Spark images

### AWS Requirements
- AWS CLI configured with credentials
- Sufficient service limits for:
  - EKS clusters (1)
  - EC2 instances (4-7 t3.medium)
  - RDS instances (1 db.t3.micro)
  - S3 buckets (3)

### Python Dependencies
```bash
pip install boto3 faker
```

## 🗂️ Project Structure

```
aws-eks-spark-iceberg/
├── eks-cluster/                 # Terraform infrastructure
│   ├── main.tf                 # VPC and EKS cluster
│   ├── hive-metastore.tf       # Hive Metastore setup
│   ├── spark.tf                # Spark cluster configuration
│   ├── s3-buckets.tf           # S3 storage setup
│   ├── variables.tf            # Terraform variables
│   └── outputs.tf              # Infrastructure outputs
├── etl-iceberg/                # ETL processing
│   ├── nginx_log_processor.py  # Main ETL script
│   └── run_spark_job.sh        # Job execution script
├── input/                      # Data generation
│   ├── log_generator.py        # Log file generator
│   └── requirements.txt        # Python dependencies
├── architecture-diagram.md     # Architecture documentation
└── README.md                   # This file
```

## 🚀 Setup Instructions

### Step 1: Configure AWS Credentials

```bash
# Configure AWS CLI
aws configure

# Verify access
aws sts get-caller-identity
```

### Step 2: Clone and Setup Project

```bash
# Clone the repository
git clone <your-repo-url>
cd aws-eks-spark-iceberg

# Install Python dependencies
cd input
python3 -m venv log-generator
source log-generator/bin/activate
pip install -r requirements.txt
cd ..
```

### Step 3: Configure Terraform Variables

Create `eks-cluster/terraform.tfvars`:

```hcl
# Required variables
cluster_name = "spark-eks-cluster"
region = "ap-south-1"
vpc_cidr = "10.0.0.0/16"

# S3 bucket names (must be globally unique)
iceberg_warehouse_bucket = "your-unique-warehouse-bucket-name"

# Database password
db_password = "your-secure-password-here"

# AWS credentials (for Spark S3 access)
aws_access_key_id = "your-access-key"
aws_secret_access_key = "your-secret-key"
```

### Step 4: Deploy Infrastructure

```bash
cd eks-cluster

# Initialize Terraform
terraform init

# Plan deployment
terraform plan

# Deploy infrastructure 
terraform apply
```

### Step 5: Configure kubectl

```bash
# Update kubeconfig
aws eks update-kubeconfig --region ap-south-1 --name spark-eks-cluster

# Verify cluster access
kubectl get nodes
kubectl get pods --all-namespaces
```

### Step 6: Wait for Services

```bash
# Wait for Hive Metastore to be ready
kubectl wait --for=condition=available --timeout=300s deployment/hive-metastore-rds

# Wait for Spark Master to be ready
kubectl wait --for=condition=available --timeout=300s deployment/spark-master-node

# Wait for Spark Workers to be ready
kubectl wait --for=condition=available --timeout=300s deployment/spark-worker

# Verify all services are running
kubectl get pods
kubectl get services
```

## 🏃‍♂️ Running the Pipeline

### Step 1: Generate Sample Data

```bash
cd input

# Generate 100 log files with 1000 lines each
python log_generator.py \
  --num-files 100 \
  --lines-per-file 1000 \
  --bucket spark-eks-cluster-input-data \
  --app nginx \
  --start-date 2025-01-01 \
  --end-date 2025-01-31

# Verify data upload
aws s3 ls s3://spark-eks-cluster-input-data/ --recursive
```

### Step 2: Execute ETL Processing

```bash
cd ../etl-iceberg

# Make script executable
chmod +x run_spark_job.sh

# Run the ETL job
./run_spark_job.sh


```

### Step 3: Monitor Job Execution

```bash
# Check Spark UI (port-forward)
kubectl port-forward service/spark-master-svc 8080:8080

# Open browser to http://localhost:8080

# Check application logs
kubectl logs -f deployment/spark-master-node
kubectl logs -f deployment/spark-worker
```

## 🔍 Querying Data

### Access Spark SQL Shell

```bash
# Start Spark SQL shell
kubectl exec -it deployment/spark-master-node -- /opt/spark/bin/spark-sql \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore-svc:9083
```

### Sample Queries

```sql
-- Show available databases
SHOW DATABASES;

-- Use nginx_logs database
USE nginx_logs;

-- Show available tables
SHOW TABLES;

-- (IMPORTANT) --> ANALYTICAL TABLES ALREADY CREATED: daily_top_ips, weekly_top_ips, daily_top_devices, weekly_top_devices

-- Basic access logs query
SELECT 
    date,
    COUNT(*) as requests,
    COUNT(DISTINCT ip_address) as unique_ips,
    AVG(response_size) as avg_response_size
FROM access_logs 
WHERE date >= '2025-01-01' 
GROUP BY date 
ORDER BY date;

-- Top IP addresses
SELECT * FROM daily_top_ips 
WHERE date = '2025-01-15' 
ORDER BY request_count DESC 
LIMIT 10;

-- Device analysis
SELECT 
    device_type,
    COUNT(*) as requests,
    COUNT(DISTINCT ip_address) as unique_users
FROM access_logs 
WHERE date BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY device_type 
ORDER BY requests DESC;

-- Status code analysis
SELECT 
    status_category,
    status_code,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM access_logs 
WHERE date >= '2025-01-01'
GROUP BY status_category, status_code 
ORDER BY count DESC;

-- Weekly analytics
SELECT * FROM weekly_analytics 
ORDER BY year, week_of_year;
```

### Query Performance Tips

```sql
-- Use partition pruning for better performance
SELECT * FROM access_logs 
WHERE date = '2025-01-15'  -- Uses partition pruning
AND hour BETWEEN 10 AND 12;

-- Use EXPLAIN to understand query plans
EXPLAIN SELECT COUNT(*) FROM access_logs WHERE date >= '2025-01-01';
```

## 📊 Monitoring and Troubleshooting

### Spark Monitoring

```bash
# Access Spark UI
kubectl port-forward service/spark-master-svc 8080:8080

# Check Spark application status
kubectl exec deployment/spark-master-node -- /opt/spark/bin/spark-submit \
  --status <application-id>

# View Spark logs
kubectl logs deployment/spark-master-node -f
kubectl logs deployment/spark-worker -f
```

### Hive Metastore Monitoring

```bash
# Check Hive Metastore logs
kubectl logs deployment/hive-metastore-rds -f

# Test Metastore connectivity
kubectl exec deployment/hive-metastore-rds -- netstat -tlnp | grep 9083

# Check database connectivity
kubectl exec deployment/hive-metastore-rds -- psql \
  -h <rds-endpoint> -U hive -d hivemetastore -c "\dt"
```



## 💡 Assumptions and Design Decisions

### Infrastructure Decisions

1. **EKS over EMR**: Chose EKS for container orchestration flexibility and cost control
2. **Spot Instances**: Used spot instances for worker nodes to reduce costs by ~70%
3. **Single NAT Gateway**: Cost optimization choice; use multiple NAT gateways for production HA
4. **Public Subnets**: EKS nodes in public subnets for simplified networking; use private subnets for production

### Storage Decisions

1. **Iceberg Format**: Chosen for ACID transactions, schema evolution, and time travel capabilities
2. **Date Partitioning**: Partitioned by date for optimal query performance
3. **S3 Storage**: Leverages S3's durability and cost-effectiveness for data lake storage
4. **Compression**: Snappy for data (fast), Gzip for metadata (space-efficient)

### Processing Decisions

1. **Spark 3.5.1**: Latest stable version with Iceberg 1.4.3 compatibility
2. **Hive Metastore**: Provides metadata management and multi-engine compatibility
3. **PostgreSQL**: Reliable and cost-effective metadata backend
4. **Python ETL**: Flexible scripting with PySpark for complex transformations

### Security Decisions

1. **Development Setup**: Public accessibility for RDS and EKS for development ease
2. **Secrets**: Kubernetes secrets for credential management
3. **IAM Policies**: Principle of least privilege for S3 access
4. **Encryption**: Server-side encryption for S3 and RDS

### Scalability Decisions

1. **Auto-scaling**: Spark workers scale 3-6 nodes based on demand
2. **Adaptive Execution**: Spark adaptive query execution for dynamic optimization
3. **File Size**: Target 128MB files for optimal S3/Spark performance
4. **Partitioning**: Date-based partitioning for query pruning