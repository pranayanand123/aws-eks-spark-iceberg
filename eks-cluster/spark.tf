# ConfigMap for Spark configuration with Hive and Iceberg support
resource "kubernetes_config_map" "spark_config" {
  metadata {
    name = "spark-config"
  }

  data = {
    "spark-defaults.conf" = <<-EOF
      # Hive Metastore Configuration
      spark.sql.catalogImplementation=hive
      spark.hadoop.hive.metastore.uris=thrift://hive-metastore-svc:9083
      
      # Iceberg Configuration
      spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
      spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
      spark.sql.catalog.spark_catalog.type=hive
      spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
      spark.sql.catalog.iceberg.type=hive
      spark.sql.catalog.iceberg.uri=thrift://hive-metastore-svc:9083
      
      # S3 Configuration
      spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
      spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
      spark.hadoop.fs.s3a.path.style.access=true
      spark.hadoop.fs.s3a.block.size=134217728
      spark.hadoop.fs.s3a.buffer.dir=/tmp
      spark.hadoop.fs.s3a.committer.name=directory
      spark.hadoop.fs.s3a.committer.staging.conflict-mode=append
      spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory
      
      # Iceberg S3 Configuration
      spark.sql.catalog.iceberg.warehouse=s3a://${var.iceberg_warehouse_bucket}/warehouse
      spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO
      
      # Performance tuning
      spark.serializer=org.apache.spark.serializer.KryoSerializer
      spark.sql.adaptive.enabled=true
      spark.sql.adaptive.coalescePartitions.enabled=true
      EOF

    "hive-site.xml" = <<-EOF
      <?xml version="1.0"?>
      <configuration>
        <property>
          <name>javax.jdo.option.ConnectionURL</name>
          <value>jdbc:postgresql://${aws_db_instance.hive_metastore.endpoint}/hivemetastore</value>
        </property>
        <property>
          <name>javax.jdo.option.ConnectionDriverName</name>
          <value>org.postgresql.Driver</value>
        </property>
        <property>
          <name>javax.jdo.option.ConnectionUserName</name>
          <value>hive</value>
        </property>
        <property>
          <name>javax.jdo.option.ConnectionPassword</name>
          <value>${var.db_password}</value>
        </property>
        <property>
          <name>hive.metastore.uris</name>
          <value>thrift://hive-metastore-svc:9083</value>
        </property>
        <property>
          <name>hive.metastore.warehouse.dir</name>
          <value>s3a://${var.iceberg_warehouse_bucket}/warehouse</value>
        </property>
        <property>
          <name>hive.exec.engine</name>
          <value>spark</value>
        </property>
      </configuration>
      EOF
  }
}

# Custom Spark Docker image with required JARs
resource "kubernetes_config_map" "spark_dockerfile" {
  metadata {
    name = "spark-dockerfile"
  }

  data = {
    "Dockerfile" = <<-EOF
      FROM apache/spark:3.5.1
      
      USER root
      
      # Install Python and pip
      RUN apt-get update && apt-get install -y python3-pip wget curl && \
          pip3 install pyspark==3.5.1 boto3 pandas pyarrow
      
      # Download required JARs
      RUN cd /opt/spark/jars && \
          wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar && \
          wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
          wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
          wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
      
      USER 185
      EOF
  }
}

resource "kubernetes_deployment" "spark_master" {
  depends_on = [module.eks, kubernetes_deployment.hive_metastore_rds]
  
  metadata {
    name = "spark-master-node"
    labels = {
      app = "spark"
      role = "master"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "spark"
        role = "master"
      }
    }

    template {
      metadata {
        labels = {
          app = "spark"
          role = "master"
        }
      }

      spec {
        # Schedule on master node group
        node_selector = {
          "spark-role" = "master"
        }

        # Tolerate the master taint
        toleration {
          key      = "spark-role"
          operator = "Equal"
          value    = "master"
          effect   = "NoSchedule"
        }

        # Init container to download JARs
        init_container {
          name  = "download-jars"
          image = "curlimages/curl:8.7.1"
          
          command = ["/bin/sh"]
          args = ["-c", <<-EOT
            cd /jars && \
            curl -L -o iceberg-spark-runtime-3.5_2.12-1.4.3.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar && \
            curl -L -o hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
            curl -L -o aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
            curl -L -o postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && \
            curl -L -o iceberg-aws-1.4.3.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.4.3/iceberg-aws-1.4.3.jar
            EOT
          ]

          volume_mount {
            name       = "spark-jars"
            mount_path = "/jars"
          }
        }

        container {
          name  = "spark-master-node"
          image = "apache/spark:3.5.1"
          command = ["/bin/bash"]
          args = ["-c", <<-EOT
            cp /shared-jars/*.jar /opt/spark/jars/ && \
            /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
            --host 0.0.0.0 --port 7077 --webui-port 8080
            EOT
          ]

          port {
            container_port = 7077
          }

          port {
            container_port = 8080
          }

          env {
            name = "SPARK_NO_DAEMONIZE"
            value = "true"
          }

          env {
            name = "SPARK_MASTER_HOST"
            value = "spark-master-svc"
          }

          env {
            name = "SPARK_MASTER_PORT"
            value = "7077"
          }

          env {
            name = "SPARK_MASTER_WEBUI_PORT"
            value = "8080"
          }

          env {
            name = "SPARK_LOCAL_HOSTNAME"
            value = "spark-master-svc"
          }

          env {
            name = "SPARK_DRIVER_HOST"
            value = "spark-master-svc"
          }

          # AWS credentials for S3 access
          env {
            name = "AWS_ACCESS_KEY_ID"
            value_from {
              secret_key_ref {
                name = "aws-credentials"
                key  = "access_key_id"
              }
            }
          }

          env {
            name = "AWS_SECRET_ACCESS_KEY"
            value_from {
              secret_key_ref {
                name = "aws-credentials"
                key  = "secret_access_key"
              }
            }
          }

          env {
            name = "AWS_DEFAULT_REGION"
            value = var.region
          }

          # Mount configuration files
          volume_mount {
            name       = "spark-config"
            mount_path = "/opt/spark/conf"
          }

          volume_mount {
            name       = "spark-jars"
            mount_path = "/shared-jars"
          }

          resources {
            requests = {
              cpu    = "500m"
              memory = "2Gi"
            }
            limits = {
              cpu    = "1000m"
              memory = "4Gi"
            }
          }
        }

        volume {
          name = "spark-config"
          config_map {
            name = kubernetes_config_map.spark_config.metadata[0].name
          }
        }

        volume {
          name = "spark-jars"
          empty_dir {}
        }
      }
    }
  }
}

resource "kubernetes_service" "spark_master_svc" {
  depends_on = [module.eks]
  
  metadata {
    name = "spark-master-svc"
  }

  spec {
    selector = {
      app  = "spark"
      role = "master"
    }

    port {
      name        = "spark-port"
      port        = 7077
      target_port = 7077
      protocol    = "TCP"
    }

    port {
      name        = "ui-port"
      port        = 8080
      target_port = 8080
      protocol    = "TCP"
    }

    # Add additional ports for driver communication
    port {
      name        = "driver-rpc"
      port        = 7078
      target_port = 7078
      protocol    = "TCP"
    }

    port {
      name        = "blockmanager"
      port        = 7079
      target_port = 7079
      protocol    = "TCP"
    }

    type = "ClusterIP"
    cluster_ip = "None"  # Headless service for better DNS resolution
  }
}

resource "kubernetes_deployment" "spark_worker" {
  depends_on = [kubernetes_deployment.spark_master]
  
  metadata {
    name = "spark-worker"
    labels = {
      app = "spark"
      role = "worker"
    }
  }

  spec {
    replicas = 3  # Match your worker node count

    selector {
      match_labels = {
        app = "spark"
        role = "worker"
      }
    }

    template {
      metadata {
        labels = {
          app = "spark"
          role = "worker"
        }
      }

      spec {
        # Schedule on worker node group
        node_selector = {
          "spark-role" = "worker"
        }

        # Tolerate the worker taint
        toleration {
          key      = "spark-role"
          operator = "Equal"
          value    = "worker"
          effect   = "NoSchedule"
        }

        # Init container to download JARs
        init_container {
          name  = "download-jars"
          image = "curlimages/curl:8.7.1"
          
          command = ["/bin/sh"]
          args = ["-c", <<-EOT
            cd /jars && \
            curl -L -o iceberg-spark-runtime-3.5_2.12-1.4.3.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar && \
            curl -L -o hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
            curl -L -o aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
            curl -L -o postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && \
            curl -L -o iceberg-aws-1.4.3.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.4.3/iceberg-aws-1.4.3.jar
            EOT
          ]

          volume_mount {
            name       = "spark-jars"
            mount_path = "/jars"
          }
        }

        container {
          name  = "spark-worker"
          image = "apache/spark:3.5.1"
          command = ["/bin/bash"]
          args = ["-c", <<-EOT
            cp /shared-jars/*.jar /opt/spark/jars/ && \
            /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
            --webui-port 8081 spark://spark-master-svc:7077
            EOT
          ]

          port {
            container_port = 8081
          }

          env {
            name = "SPARK_NO_DAEMONIZE"
            value = "true"
          }

          env {
            name = "SPARK_WORKER_CORES"
            value = "1"
          }

          env {
            name = "SPARK_WORKER_MEMORY"
            value = "2g"
          }

          env {
            name = "SPARK_WORKER_WEBUI_PORT"
            value = "8081"
          }

          # Add this to help with worker registration
          env {
            name = "SPARK_LOCAL_IP"
            value_from {
              field_ref {
                field_path = "status.podIP"
              }
            }
          }

          # AWS credentials for S3 access
          env {
            name = "AWS_ACCESS_KEY_ID"
            value_from {
              secret_key_ref {
                name = "aws-credentials"
                key  = "access_key_id"
              }
            }
          }

          env {
            name = "AWS_SECRET_ACCESS_KEY"
            value_from {
              secret_key_ref {
                name = "aws-credentials"
                key  = "secret_access_key"
              }
            }
          }

          env {
            name = "AWS_DEFAULT_REGION"
            value = var.region
          }

          # Mount configuration files
          volume_mount {
            name       = "spark-config"
            mount_path = "/opt/spark/conf"
          }

          volume_mount {
            name       = "spark-jars"
            mount_path = "/shared-jars"
          }

          resources {
            requests = {
              cpu    = "500m"
              memory = "1Gi"
            }
            limits = {
              cpu    = "1000m"
              memory = "2Gi"
            }
          }
        }

        volume {
          name = "spark-config"
          config_map {
            name = kubernetes_config_map.spark_config.metadata[0].name
          }
        }

        volume {
          name = "spark-jars"
          empty_dir {}
        }
      }
    }
  }
}