module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.1.2"

  name = "spark-vpc"
  cidr = var.vpc_cidr

  azs             = ["${var.region}a", "${var.region}b", "${var.region}c"]
  public_subnets  = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  private_subnets = ["10.0.11.0/24", "10.0.12.0/24", "10.0.13.0/24"]

  enable_nat_gateway = true
  single_nat_gateway = true
  map_public_ip_on_launch = true
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "20.8.5"

  cluster_name    = var.cluster_name
  cluster_version = "1.29"

  subnet_ids         = module.vpc.public_subnets
  vpc_id             = module.vpc.vpc_id
  enable_irsa        = false
  cluster_endpoint_public_access  = true
  cluster_endpoint_public_access_cidrs = ["0.0.0.0/0"]


  # Add access entries for your IAM user
  access_entries = {
    terraform-user = {
      principal_arn = "arn:aws:iam::914936021803:user/terraform-pranay"
      policy_associations = {
        admin = {
          policy_arn = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
          access_scope = {
            type = "cluster"
          }
        }
      }
    }
  }

  eks_managed_node_groups = {

    default = {
      min_size     = 1
      max_size     = 1
      desired_size = 1

      instance_types = ["t3.medium"]
      capacity_type  = "ON_DEMAND"
    }

    default_master = {
      min_size     = 1
      max_size     = 1
      desired_size = 1

      instance_types = ["t3.medium"]
      capacity_type  = "ON_DEMAND"

      # Add labels for master nodes
      labels = {
        "node-type" = "spark-master"
        "spark-role" = "master"
      }

      # Add taint to ensure only spark master pods are scheduled here
      taints = [
        {
          key    = "spark-role"
          value  = "master"
          effect = "NO_SCHEDULE"
        }
      ]
    }
    default_worker = {
      min_size     = 3
      max_size     = 6
      desired_size = 3

      instance_types = ["t3.medium"]
      capacity_type  = "SPOT"

      # Add labels for worker nodes
      labels = {
        "node-type" = "spark-worker"
        "spark-role" = "worker"
      }

      # Add taint to ensure only spark worker pods are scheduled here
      taints = [
        {
          key    = "spark-role"
          value  = "worker"
          effect = "NO_SCHEDULE"
        }
      ]
    }
  }

  tags = {
    Environment = "dev"
    Project     = "spark-on-eks"
  }
}
