resource "kubernetes_deployment" "spark_master" {
  depends_on = [module.eks]
  
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

        container {
          name  = "spark-master-node"
          image = "apache/spark:3.5.1"
          command = ["/opt/spark/bin/spark-class"]
          args = ["org.apache.spark.deploy.master.Master", "--host", "0.0.0.0", "--port", "7077", "--webui-port", "8080"]

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
            value = "0.0.0.0"
          }

          env {
            name = "SPARK_MASTER_PORT"
            value = "7077"
          }

          env {
            name = "SPARK_MASTER_WEBUI_PORT"
            value = "8080"
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

    type = "LoadBalancer"
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

        container {
          name  = "spark-worker"
          image = "apache/spark:3.5.1"
          command = ["/opt/spark/bin/spark-class"]
          args = ["org.apache.spark.deploy.worker.Worker", "--webui-port", "8081", "spark://spark-master-svc:7077"]

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
            value = "1g"
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
      }
    }
  }
}