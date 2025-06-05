#!/bin/bash

SCRIPT_NAME="nginx_log_processor.py"
POD_NAME=$(kubectl get pods -l app=spark,role=master -o jsonpath='{.items[0].metadata.name}')

# Copy script to pod and execute
kubectl cp $SCRIPT_NAME $POD_NAME:/tmp/$SCRIPT_NAME && \
kubectl exec -it $POD_NAME -- /opt/spark/bin/spark-submit \
  --master spark://spark-master-svc:7077 \
  --conf spark.driver.host=spark-master-svc \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=0 \
  --conf spark.blockManager.port=0 \
  --conf spark.sql.warehouse.dir=s3a://spark-eks-cluster-warehouse-data/warehouse \
  --conf spark.hadoop.hive.metastore.warehouse.dir=s3a://spark-eks-cluster-warehouse-data/warehouse \
  --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/postgresql-42.6.0.jar,/opt/spark/jars/iceberg-aws-1.4.3.jar \
  --conf spark.executor.extraClassPath=/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar:/opt/spark/jars/iceberg-aws-1.4.3.jar \
  --conf spark.driver.extraClassPath=/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar:/opt/spark/jars/iceberg-aws-1.4.3.jar \
  --conf spark.jars.packages="" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hive \
  --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore-svc:9083 \
  --conf spark.sql.catalog.iceberg.warehouse=s3a://spark-eks-cluster-warehouse-data/warehouse \
  /tmp/$SCRIPT_NAME \
  --input-path s3a://spark-eks-cluster-input-data/logs/nginx/*/*/*/*.log.gz

