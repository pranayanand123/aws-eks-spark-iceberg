resource "kubernetes_secret" "aws_credentials" {
  metadata {
    name = "aws-credentials"
  }

  data = {
    access_key_id     = var.aws_access_key_id
    secret_access_key = var.aws_secret_access_key
  }

  type = "Opaque"
}

# RDS PostgreSQL for Hive Metastore
resource "aws_db_subnet_group" "hive_metastore" {
  name       = "hive-metastore-subnet-group"
  subnet_ids = module.vpc.public_subnets

  tags = {
    Name = "Hive Metastore DB subnet group"
  }
}

resource "aws_security_group" "hive_metastore_rds" {
  name_prefix = "hive-metastore-rds"
  vpc_id      = module.vpc.vpc_id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "hive-metastore-rds-sg"
  }
}

resource "aws_db_instance" "hive_metastore" {
  identifier = "hive-metastore-db"
  
  engine         = "postgres"
  engine_version = "17.1"
  instance_class = "db.t3.micro"
  
  allocated_storage     = 20
  max_allocated_storage = 100
  storage_encrypted     = true
  
  db_name  = "hivemetastore"
  username = "hive"
  password = var.db_password

  publicly_accessible = true
  
  vpc_security_group_ids = [aws_security_group.hive_metastore_rds.id]
  db_subnet_group_name   = aws_db_subnet_group.hive_metastore.name
  
  backup_retention_period = 7
  backup_window          = "03:00-04:00"
  maintenance_window     = "sun:04:00-sun:05:00"
  
  skip_final_snapshot = true
  deletion_protection = false
  
  tags = {
    Name = "hive-metastore-db"
  }
}

# Kubernetes Secret for database credentials
resource "kubernetes_secret" "hive_db_credentials" {
  metadata {
    name = "hive-db-credentials"
  }

  data = {
    username = base64encode("hive")
    password = base64encode(var.db_password)
    host     = base64encode(aws_db_instance.hive_metastore.endpoint)
    port     = base64encode("5432")
    database = base64encode("hivemetastore")
  }

  type = "Opaque"
}

resource "kubernetes_config_map" "hive_config_rds" {
  metadata {
    name = "hive-config"
  }

  data = {
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
          <value>thrift://0.0.0.0:9083</value>
        </property>
        <property>
          <name>hive.metastore.warehouse.dir</name>
          <value>s3a://${var.iceberg_warehouse_bucket}/warehouse</value>
        </property>
        <property>
          <name>hive.exec.engine</name>
          <value>spark</value>
        </property>
        <property>
          <name>hive.metastore.schema.verification</name>
          <value>true</value>
        </property>
        <property>
          <name>datanucleus.autoCreateSchema</name>
          <value>false</value>
        </property>
        <property>
          <name>hive.metastore.schema.verification.record.version</name>
          <value>true</value>
        </property>
        
        <!-- Disable StorageBasedAuthorizationProvider -->
        <property>
          <name>hive.security.authorization.enabled</name>
          <value>false</value>
        </property>
        <property>
          <name>hive.metastore.pre.event.listeners</name>
          <value></value>
        </property>
        <property>
          <name>hive.security.authorization.manager</name>
          <value>org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory</value>
        </property>
        
        <!-- Enable external database creation -->
        <property>
          <name>hive.metastore.try.direct.sql</name>
          <value>true</value>
        </property>
        <property>
          <name>hive.metastore.try.direct.sql.ddl</name>
          <value>true</value>
        </property>
        
        <!-- S3A Configuration for Hive -->
        <property>
          <name>fs.s3a.impl</name>
          <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
        </property>
        <property>
          <name>fs.s3a.aws.credentials.provider</name>
          <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
        </property>
        <property>
          <name>fs.s3a.access.key</name>
          <value>${var.aws_access_key_id}</value>
        </property>
        <property>
          <name>fs.s3a.secret.key</name>
          <value>${var.aws_secret_access_key}</value>
        </property>
        <property>
          <name>fs.s3a.endpoint.region</name>
          <value>${var.region}</value>
        </property>
        <property>
          <name>fs.s3a.path.style.access</name>
          <value>true</value>
        </property>
        <property>
          <name>fs.s3a.connection.ssl.enabled</name>
          <value>true</value>
        </property>
        <property>
          <name>fs.s3a.fast.upload</name>
          <value>true</value>
        </property>
        <property>
          <name>fs.s3a.block.size</name>
          <value>134217728</value>
        </property>
        <property>
          <name>fs.s3a.buffer.dir</name>
          <value>/tmp</value>
        </property>
        <property>
          <name>fs.s3a.committer.name</name>
          <value>directory</value>
        </property>
        
        <!-- External warehouse directory -->
        <property>
          <name>hive.metastore.warehouse.external.dir</name>
          <value>s3a://${var.iceberg_warehouse_bucket}/warehouse</value>
        </property>
        <property>
          <name>hive.metastore.create.as.acid</name>
          <value>false</value>
        </property>
        
        <!-- Bypass directory creation issues -->
        <property>
          <name>hive.metastore.fshandler.threads</name>
          <value>10</value>
        </property>
        <property>
          <name>hive.warehouse.subdir.inherit.perms</name>
          <value>false</value>
        </property>
      </configuration>
      EOF
  }
}

# Hive Metastore Deployment with PostgreSQL
resource "kubernetes_deployment" "hive_metastore_rds" {
  depends_on = [aws_db_instance.hive_metastore]
  
  metadata {
    name = "hive-metastore"
    labels = {
      app = "hive-metastore"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "hive-metastore"
      }
    }

    template {
      metadata {
        labels = {
          app = "hive-metastore"
        }
      }

      spec {
        # Schedule on worker node group
        node_selector = {
          "spark-role" = "worker"
        }

        toleration {
          key      = "spark-role"
          operator = "Equal"
          value    = "worker"
          effect   = "NoSchedule"
        }

        # Init container to download all required JARs
        init_container {
        name  = "download-jars"
        image = "curlimages/curl:8.7.1"

        command = ["/bin/sh"]
        args = ["-c", <<-EOT
            cd /jars && \
            echo "Downloading PostgreSQL JDBC driver..." && \
            curl -L -o postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && \
            echo "Downloading Hadoop AWS JAR..." && \
            curl -L -o hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
            echo "Downloading AWS Java SDK Bundle..." && \
            curl -L -o aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
            echo "Downloading Iceberg Hive Runtime JAR..." && \
            curl -L -o iceberg-hive-runtime-1.4.3.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/1.4.3/iceberg-hive-runtime-1.4.3.jar && \
            echo "Downloading Iceberg AWS JAR..." && \
            curl -L -o iceberg-aws-1.4.3.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.4.3/iceberg-aws-1.4.3.jar && \
            echo "All JARs downloaded successfully!" && \
            ls -la /jars/
            EOT
        ]

        volume_mount {
            name       = "hive-jars"
            mount_path = "/jars"
        }

        resources {
            requests = {
            cpu    = "100m"
            memory = "100Mi"
            }
            limits = {
            cpu    = "500m"
            memory = "500Mi"
            }
        }
        }

        # Init container to initialize PostgreSQL schema
        init_container {
          name  = "schema-init"
          image = "apache/hive:4.0.0"

          command = ["/bin/bash"]
          args = ["-c", <<-EOT
            echo "Setting up HIVE_AUX_JARS_PATH to include downloaded JARs..." && \
            export HIVE_AUX_JARS_PATH="/extra-jars" && \
            echo "Checking if schema exists..." && \
            if /opt/hive/bin/schematool -dbType postgres -info; then \
              echo "Schema already exists, skipping initialization"; \
            else \
              echo "Initializing Hive schema..." && \
              /opt/hive/bin/schematool -dbType postgres -initSchema --verbose; \
            fi && \
            echo "Schema initialization completed successfully"
            EOT
          ]

          env {
            name  = "HIVE_HOME"
            value = "/opt/hive"
          }

          env {
            name  = "HIVE_AUX_JARS_PATH"
            value = "/extra-jars"
          }

          env {
            name  = "HIVE_CONF_DIR"
            value = "/opt/hive/conf"
          }

          # Add AWS credentials for schema initialization
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

          volume_mount {
            name       = "hive-config"
            mount_path = "/opt/hive/conf"
          }

          volume_mount {
            name       = "hive-jars"
            mount_path = "/extra-jars"
          }

          resources {
            requests = {
              cpu    = "250m"
              memory = "512Mi"
            }
            limits = {
              cpu    = "500m"
              memory = "1Gi"
            }
          }
        }

        container {
          name  = "hive-metastore"
          image = "apache/hive:4.0.0"
          
          command = ["/bin/bash"]
          args = ["-c", <<-EOT
            echo "Using JARs from HIVE_AUX_JARS_PATH: /extra-jars" && \
            ls -la /extra-jars/ && \
            echo "Starting Hive Metastore service..." && \
            /opt/hive/bin/hive --service metastore
            EOT
          ]

          port {
            container_port = 9083
          }

          env {
            name = "HIVE_HOME"
            value = "/opt/hive"
          }

          env {
            name  = "HIVE_AUX_JARS_PATH"
            value = "/extra-jars"
          }

          # Add AWS credentials for S3 access
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

          # Mount Hive configuration
          volume_mount {
            name       = "hive-config"
            mount_path = "/opt/hive/conf"
          }

          volume_mount {
            name       = "hive-jars"
            mount_path = "/extra-jars"
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

          # Health check
          liveness_probe {
            tcp_socket {
              port = 9083
            }
            initial_delay_seconds = 240
            period_seconds        = 30
            timeout_seconds       = 10
            failure_threshold     = 3
          }

          readiness_probe {
            tcp_socket {
              port = 9083
            }
            initial_delay_seconds = 180
            period_seconds        = 10
            timeout_seconds       = 5
            failure_threshold     = 3
          }
        }

        # Volumes
        volume {
          name = "hive-config"
          config_map {
            name = kubernetes_config_map.hive_config_rds.metadata[0].name
          }
        }

        volume {
          name = "hive-jars"
          empty_dir {}
        }
      }
    }
  }
}

# Hive Metastore Service
resource "kubernetes_service" "hive_metastore_svc" {
  depends_on = [module.eks]
  
  metadata {
    name = "hive-metastore-svc"
  }

  spec {
    selector = {
      app = "hive-metastore"
    }

    port {
      name        = "metastore-port"
      port        = 9083
      target_port = 9083
      protocol    = "TCP"
    }

    type = "ClusterIP"
  }
}
