#!/usr/bin/env python3

"""
Optimized Nginx Log Processing Script for EKS Spark Cluster
Processes nginx logs from S3, transforms data, and stores in Iceberg format
with daily/weekly analytics tables.
"""

import sys
import re
import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

def create_spark_session(app_name="NginxLogProcessor"):
    """Create Spark session with Iceberg and S3 configurations"""
    
    # Stop existing session if any
    try:
        from pyspark import SparkContext
        if SparkContext._active_spark_context:
            SparkContext._active_spark_context.stop()
    except:
        pass
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore-svc:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://spark-eks-cluster-warehouse-data/warehouse") \
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
        .config("spark.sql.warehouse.dir", "s3a://spark-eks-cluster-warehouse-data/warehouse") \
        .config("spark.hadoop.hive.metastore.warehouse.dir", "s3a://spark-eks-cluster-warehouse-data/warehouse") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1") \
        .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "true") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.sql.files.minPartitionNum", "1") \
        .config("spark.default.parallelism", "3") \
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("INFO")
    return spark

def parse_nginx_log_line(log_line):
    """Parse nginx log format"""
    # Nginx combined log format pattern
    pattern = r'^(\S+) \S+ \S+ \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+|-) "([^"]*)" "([^"]*)"'
    
    match = re.match(pattern, log_line)
    if match:
        return {
            'ip_address': match.group(1),
            'timestamp_str': match.group(2),
            'method': match.group(3),
            'url': match.group(4),
            'protocol': match.group(5),
            'status_code': int(match.group(6)),
            'response_size': int(match.group(7)) if match.group(7) != '-' else 0,
            'referer': match.group(8) if match.group(8) != '-' else None,
            'user_agent': match.group(9) if match.group(9) != '-' else None
        }
    return None

def extract_device_info(user_agent):
    """Extract device information from user agent string"""
    if not user_agent:
        return "Unknown"
    
    user_agent_lower = user_agent.lower()
    
    # Mobile devices
    if any(mobile in user_agent_lower for mobile in ['mobile', 'android', 'iphone', 'ipod']):
        if 'android' in user_agent_lower:
            return "Android Mobile"
        elif any(ios in user_agent_lower for ios in ['iphone', 'ipod']):
            return "iOS Mobile"
        else:
            return "Mobile"
    
    # Tablets
    if any(tablet in user_agent_lower for tablet in ['ipad', 'tablet']):
        if 'ipad' in user_agent_lower:
            return "iPad"
        else:
            return "Tablet"
    
    # Desktop browsers
    if 'chrome' in user_agent_lower:
        return "Chrome Desktop"
    elif 'firefox' in user_agent_lower:
        return "Firefox Desktop"
    elif 'safari' in user_agent_lower and 'chrome' not in user_agent_lower:
        return "Safari Desktop"
    elif 'edge' in user_agent_lower:
        return "Edge Desktop"
    elif 'opera' in user_agent_lower:
        return "Opera Desktop"
    
    # Bots and crawlers
    if any(bot in user_agent_lower for bot in ['bot', 'crawler', 'spider', 'scraper']):
        return "Bot/Crawler"
    
    return "Other Desktop"

def setup_iceberg_database(spark):
    """Setup Iceberg database and tables"""
    
    print("Setting up Iceberg database and tables...")
    
    # Try to create the database
    try:
        spark.sql("""
            CREATE DATABASE IF NOT EXISTS nginx_logs
            LOCATION 's3a://spark-eks-cluster-warehouse-data/warehouse/nginx_logs.db'
        """)
        print("✓ Database nginx_logs created successfully")
    except Exception as e:
        print(f"Warning: Could not create database with location: {e}")
        try:
            spark.sql("CREATE DATABASE IF NOT EXISTS nginx_logs")
            print("✓ Database nginx_logs created without explicit location")
        except Exception as e2:
            print(f"Error creating database: {e2}")
            print("Using default database instead...")
    
    # Set current database
    try:
        spark.sql("USE nginx_logs")
        print("✓ Using nginx_logs database")
    except:
        print("✓ Using default database")
    
    # Main access logs table - FIXED: Partitioned by date only
    create_access_logs_sql = """
    CREATE TABLE IF NOT EXISTS access_logs (
        ip_address string,
        timestamp timestamp,
        method string,
        url string,
        protocol string,
        status_code int,
        response_size bigint,
        referer string,
        user_agent string,
        device_type string,
        date date,
        hour int,
        year int,
        month int,
        day int,
        week_of_year int,
        status_category string,
        url_path string,
        file_extension string,
        is_bot boolean,
        processed_timestamp timestamp
    ) USING iceberg
    PARTITIONED BY (date)
    TBLPROPERTIES (
        'write.target-file-size-bytes'='134217728',
        'write.metadata.compression-codec'='gzip',
        'write.data.compression-codec'='snappy'
    )
    LOCATION 's3a://spark-eks-cluster-warehouse-data/warehouse/nginx_logs/access_logs'
    """
    
    # Daily top IPs table
    create_daily_top_ips_sql = """
    CREATE TABLE IF NOT EXISTS daily_top_ips (
        date date,
        ip_address string,
        request_count bigint,
        rank int,
        processed_timestamp timestamp
    ) USING iceberg
    PARTITIONED BY (date)
    TBLPROPERTIES (
        'write.target-file-size-bytes'='67108864',
        'write.metadata.compression-codec'='gzip'
    )
    LOCATION 's3a://spark-eks-cluster-warehouse-data/warehouse/nginx_logs/daily_top_ips'
    """
    
    # Weekly top IPs table
    create_weekly_top_ips_sql = """
    CREATE TABLE IF NOT EXISTS weekly_top_ips (
        week_start_date date,
        week_end_date date,
        year int,
        week_of_year int,
        ip_address string,
        request_count bigint,
        rank int,
        processed_timestamp timestamp
    ) USING iceberg
    PARTITIONED BY (year, week_of_year)
    TBLPROPERTIES (
        'write.target-file-size-bytes'='67108864',
        'write.metadata.compression-codec'='gzip'
    )
    LOCATION 's3a://spark-eks-cluster-warehouse-data/warehouse/nginx_logs/weekly_top_ips'
    """
    
    # Daily top devices table
    create_daily_top_devices_sql = """
    CREATE TABLE IF NOT EXISTS daily_top_devices (
        date date,
        device_type string,
        request_count bigint,
        rank int,
        processed_timestamp timestamp
    ) USING iceberg
    PARTITIONED BY (date)
    TBLPROPERTIES (
        'write.target-file-size-bytes'='67108864',
        'write.metadata.compression-codec'='gzip'
    )
    LOCATION 's3a://spark-eks-cluster-warehouse-data/warehouse/nginx_logs/daily_top_devices'
    """
    
    # Weekly top devices table
    create_weekly_top_devices_sql = """
    CREATE TABLE IF NOT EXISTS weekly_top_devices (
        week_start_date date,
        week_end_date date,
        year int,
        week_of_year int,
        device_type string,
        request_count bigint,
        rank int,
        processed_timestamp timestamp
    ) USING iceberg
    PARTITIONED BY (year, week_of_year)
    TBLPROPERTIES (
        'write.target-file-size-bytes'='67108864',
        'write.metadata.compression-codec'='gzip'
    )
    LOCATION 's3a://spark-eks-cluster-warehouse-data/warehouse/nginx_logs/weekly_top_devices'
    """
    
    # Execute table creation
    tables = [
        ("access_logs", create_access_logs_sql),
        ("daily_top_ips", create_daily_top_ips_sql),
        ("weekly_top_ips", create_weekly_top_ips_sql),
        ("daily_top_devices", create_daily_top_devices_sql),
        ("weekly_top_devices", create_weekly_top_devices_sql)
    ]
    
    for table_name, sql in tables:
        try:
            spark.sql(sql)
            print(f"✓ Table {table_name} created/verified")
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")
            raise
    
    print("All Iceberg tables created successfully!")

def process_raw_logs(spark, input_path):
    """Process raw nginx logs and return enriched DataFrame"""
    
    print(f"Reading raw logs from: {input_path}")
    
    # Read compressed log files with optimizations
    raw_logs = spark.read \
        .option("multiline", "false") \
        .option("lineSep", "\n") \
        .text(input_path)
    
    total_lines = raw_logs.count()
    print(f"Total log lines read: {total_lines}")
    
    if total_lines == 0:
        print("No log lines found!")
        return None
    
    # Define schema for parsed logs
    log_schema = StructType([
        StructField("ip_address", StringType(), True),
        StructField("timestamp_str", StringType(), True),
        StructField("method", StringType(), True),
        StructField("url", StringType(), True),
        StructField("protocol", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("response_size", LongType(), True),
        StructField("referer", StringType(), True),
        StructField("user_agent", StringType(), True)
    ])
    
    # UDFs for parsing
    parse_log_udf = udf(parse_nginx_log_line, log_schema)
    extract_device_udf = udf(extract_device_info, StringType())
    
    # Parse and clean the logs with caching for reuse
    parsed_logs = raw_logs.select(
        parse_log_udf(col("value")).alias("parsed")
    ).select("parsed.*") \
    .filter(col("ip_address").isNotNull()) \
    .cache()  # Cache for reuse
    
    parsed_count = parsed_logs.count()
    print(f"Successfully parsed log lines: {parsed_count}")
    
    if parsed_count == 0:
        print("No valid log lines found after parsing!")
        return None
    
    # Transform and enrich data with optimizations
    enriched_logs = parsed_logs.select(
        col("ip_address"),
        # Parse timestamp with timezone
        to_timestamp(col("timestamp_str"), "dd/MMM/yyyy:HH:mm:ss Z").alias("timestamp"),
        col("method"),
        col("url"),
        col("protocol"),
        col("status_code"),
        col("response_size"),
        col("referer"),
        col("user_agent")
    ).filter(col("timestamp").isNotNull()) \
    .withColumn("device_type", extract_device_udf(col("user_agent"))) \
    .withColumn("date", to_date(col("timestamp"))) \
    .withColumn("hour", hour(col("timestamp"))) \
    .withColumn("year", year(col("timestamp"))) \
    .withColumn("month", month(col("timestamp"))) \
    .withColumn("day", dayofmonth(col("timestamp"))) \
    .withColumn("week_of_year", weekofyear(col("timestamp"))) \
    .withColumn(
        "status_category",
        when(col("status_code") < 300, "Success")
        .when(col("status_code") < 400, "Redirect")
        .when(col("status_code") < 500, "Client Error")
        .otherwise("Server Error")
    ).withColumn(
        "url_path",
        regexp_extract(col("url"), r"^([^?]*)", 1)
    ).withColumn(
        "file_extension",
        regexp_extract(col("url"), r"\.([a-zA-Z0-9]+)(?:\?|$)", 1)
    ).withColumn(
        "is_bot",
        lower(col("user_agent")).rlike("bot|crawler|spider|scraper|slurp")
    ).withColumn(
        "processed_timestamp",
        current_timestamp()
    )
    
    # Repartition by date for better write performance
    enriched_logs = enriched_logs.repartition(3, col("date"))
    
    enriched_count = enriched_logs.count()
    print(f"Enriched log lines: {enriched_count}")
    
    # Unpersist the cached parsed_logs
    parsed_logs.unpersist()
    
    return enriched_logs

def store_main_logs(spark, enriched_logs):
    """Store main access logs in Iceberg format"""
    
    print("Storing main access logs to Iceberg...")
    
    # Write to main access logs table with optimization
    enriched_logs.writeTo("access_logs") \
        .option("write-audit-publish", "true") \
        .option("write.distribution-mode", "hash") \
        .append()
    
    print("✓ Main access logs stored successfully")

def generate_daily_analytics(spark):
    """Generate daily top 5 analytics with optimized queries"""
    
    print("Generating daily analytics...")
    
    # Get date range from actual data instead of hardcoded 30 days
    date_range = spark.sql("""
        SELECT 
            MIN(date) as min_date,
            MAX(date) as max_date,
            COUNT(DISTINCT date) as total_days
        FROM access_logs
    """).collect()[0]
    
    print(f"   📅 Processing data from {date_range['min_date']} to {date_range['max_date']} ({date_range['total_days']} days)")
    
    # Daily top 5 IPs - optimized with broadcast hint for small tables
    daily_top_ips = spark.sql("""
        WITH daily_ip_counts AS (
            SELECT 
                date,
                ip_address,
                COUNT(*) as request_count
            FROM access_logs 
            GROUP BY date, ip_address
        ),
        ranked_ips AS (
            SELECT 
                date,
                ip_address,
                request_count,
                ROW_NUMBER() OVER (PARTITION BY date ORDER BY request_count DESC) as rank
            FROM daily_ip_counts
        )
        SELECT 
            date,
            ip_address,
            request_count,
            rank,
            current_timestamp() as processed_timestamp
        FROM ranked_ips 
        WHERE rank <= 5
        ORDER BY date DESC, rank
    """)
    
    # Store daily top IPs
    daily_top_ips.writeTo("daily_top_ips") \
        .option("write-audit-publish", "true") \
        .overwritePartitions()
    
    print(f"   ✅ Stored daily top IPs: {daily_top_ips.count()} records")
    
    # Daily top 5 devices
    daily_top_devices = spark.sql("""
        WITH daily_device_counts AS (
            SELECT 
                date,
                device_type,
                COUNT(*) as request_count
            FROM access_logs 
            GROUP BY date, device_type
        ),
        ranked_devices AS (
            SELECT 
                date,
                device_type,
                request_count,
                ROW_NUMBER() OVER (PARTITION BY date ORDER BY request_count DESC) as rank
            FROM daily_device_counts
        )
        SELECT 
            date,
            device_type,
            request_count,
            rank,
            current_timestamp() as processed_timestamp
        FROM ranked_devices 
        WHERE rank <= 5
        ORDER BY date DESC, rank
    """)
    
    # Store daily top devices
    daily_top_devices.writeTo("daily_top_devices") \
        .option("write-audit-publish", "true") \
        .overwritePartitions()
    
    print(f"   ✅ Stored daily top devices: {daily_top_devices.count()} records")
    print("✓ Daily analytics generated and stored")

def generate_weekly_analytics(spark):
    """Generate weekly top 5 analytics with optimized queries"""
    
    print("Generating weekly analytics...")
    
    # Weekly top 5 IPs - using actual data range
    weekly_top_ips = spark.sql("""
        WITH weekly_ip_counts AS (
            SELECT 
                year,
                week_of_year,
                date_sub(next_day(date, 'MON'), 7) as week_start_date,
                next_day(date_sub(next_day(date, 'MON'), 7), 'SUN') as week_end_date,
                ip_address,
                COUNT(*) as request_count
            FROM access_logs 
            GROUP BY year, week_of_year, 
                     date_sub(next_day(date, 'MON'), 7),
                     next_day(date_sub(next_day(date, 'MON'), 7), 'SUN'),
                     ip_address
        ),
        ranked_ips AS (
            SELECT 
                year,
                week_of_year,
                week_start_date,
                week_end_date,
                ip_address,
                request_count,
                ROW_NUMBER() OVER (PARTITION BY year, week_of_year ORDER BY request_count DESC) as rank
            FROM weekly_ip_counts
        )
        SELECT 
            week_start_date,
            week_end_date,
            year,
            week_of_year,
            ip_address,
            request_count,
            rank,
            current_timestamp() as processed_timestamp
        FROM ranked_ips 
        WHERE rank <= 5
        ORDER BY year DESC, week_of_year DESC, rank
    """)
    
    # Store weekly top IPs
    weekly_top_ips.writeTo("weekly_top_ips") \
        .option("write-audit-publish", "true") \
        .overwritePartitions()
    
    print(f"   ✅ Stored weekly top IPs: {weekly_top_ips.count()} records")
    
    # Weekly top 5 devices
    weekly_top_devices = spark.sql("""
        WITH weekly_device_counts AS (
            SELECT 
                year,
                week_of_year,
                date_sub(next_day(date, 'MON'), 7) as week_start_date,
                next_day(date_sub(next_day(date, 'MON'), 7), 'SUN') as week_end_date,
                device_type,
                COUNT(*) as request_count
            FROM access_logs 
            GROUP BY year, week_of_year,
                     date_sub(next_day(date, 'MON'), 7),
                     next_day(date_sub(next_day(date, 'MON'), 7), 'SUN'),
                     device_type
        ),
        ranked_devices AS (
            SELECT 
                year,
                week_of_year,
                week_start_date,
                week_end_date,
                device_type,
                request_count,
                ROW_NUMBER() OVER (PARTITION BY year, week_of_year ORDER BY request_count DESC) as rank
            FROM weekly_device_counts
        )
        SELECT 
            week_start_date,
            week_end_date,
            year,
            week_of_year,
            device_type,
            request_count,
            rank,
            current_timestamp() as processed_timestamp
        FROM ranked_devices 
        WHERE rank <= 5
        ORDER BY year DESC, week_of_year DESC, rank
    """)
    
    # Store weekly top devices
    weekly_top_devices.writeTo("weekly_top_devices") \
        .option("write-audit-publish", "true") \
        .overwritePartitions()
    
    print(f"   ✅ Stored weekly top devices: {weekly_top_devices.count()} records")
    print("✓ Weekly analytics generated and stored")

def show_summary_stats(spark):
    """Display summary statistics"""
    
    print("\n" + "="*60)
    print("PROCESSING SUMMARY")
    print("="*60)
    
    # Total records processed
    total_records = spark.sql("SELECT COUNT(*) as count FROM access_logs").collect()[0]['count']
    print(f"Total records in access_logs: {total_records:,}")
    
    # Data range summary
    print("\n--- Data Range Summary ---")
    spark.sql("""
        SELECT 
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            COUNT(DISTINCT date) as total_days,
            COUNT(DISTINCT ip_address) as unique_ips,
            AVG(response_size) as avg_response_size
        FROM access_logs
    """).show(truncate=False)
    
    # Recent data summary (last 300 days or available days)
    print("\n--- Recent Daily Summary ---")
    spark.sql("""
        SELECT 
            date,
            COUNT(*) as total_requests,
            COUNT(DISTINCT ip_address) as unique_ips,
            ROUND(AVG(response_size), 2) as avg_response_size,
            SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as error_count
        FROM access_logs 
        WHERE date >= (SELECT MAX(date) - 300 FROM access_logs)
        GROUP BY date 
        ORDER BY date DESC
        LIMIT 20
    """).show(truncate=False)
    
    # Analytics table summary
    print("\n--- Analytics Tables Summary ---")
    
    # Daily top IPs summary
    daily_count = spark.sql("SELECT COUNT(*) as count FROM daily_top_ips").collect()[0]['count']
    print(f"Daily top IPs records: {daily_count:,}")
    
    # Weekly analytics summary
    weekly_ips_count = spark.sql("SELECT COUNT(*) as count FROM weekly_top_ips").collect()[0]['count']
    weekly_devices_count = spark.sql("SELECT COUNT(*) as count FROM weekly_top_devices").collect()[0]['count']
    daily_devices_count = spark.sql("SELECT COUNT(*) as count FROM daily_top_devices").collect()[0]['count']
    
    print(f"Weekly top IPs records: {weekly_ips_count:,}")
    print(f"Weekly top devices records: {weekly_devices_count:,}")
    print(f"Daily top devices records: {daily_devices_count:,}")
    
    # Sample of latest analytics
    print("\n--- Latest Daily Top IPs (Sample) ---")
    spark.sql("""
        SELECT date, ip_address, request_count, rank
        FROM daily_top_ips 
        WHERE date = (SELECT MAX(date) FROM daily_top_ips)
        ORDER BY rank
        LIMIT 5
    """).show(truncate=False)
    
    # Sample of latest devices
    print("\n--- Latest Daily Top Devices (Sample) ---")
    spark.sql("""
        SELECT date, device_type, request_count, rank
        FROM daily_top_devices 
        WHERE date = (SELECT MAX(date) FROM daily_top_devices)
        ORDER BY rank
        LIMIT 5
    """).show(truncate=False)
    
    print("="*60)

def main():
    """Main processing function"""
    
    parser = argparse.ArgumentParser(description='Process nginx logs with Spark and Iceberg')
    parser.add_argument('--input-path', 
                       default='s3a://spark-eks-cluster-input-data/logs/nginx/*/*/*/*.log.gz',
                       help='S3 path to input nginx log files')
    parser.add_argument('--app-name', 
                       default='NginxLogProcessor', 
                       help='Spark application name')
    parser.add_argument('--process-date', 
                       help='Specific date to process (YYYY-MM-DD format). If not provided, processes all available data')
    
    args = parser.parse_args()
    
    # Adjust input path for specific date if provided
    if args.process_date:
        try:
            date_obj = datetime.strptime(args.process_date, '%Y-%m-%d')
            year = date_obj.year
            month = date_obj.month
            day = date_obj.day
            args.input_path = f"s3a://spark-eks-cluster-input-data/logs/nginx/{year}/{month:02d}/{day:02d}/*.log.gz"
            print(f"Processing logs for date: {args.process_date}")
        except ValueError:
            print("Error: Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    
    # Create Spark session
    spark = create_spark_session(args.app_name)
    
    try:
        print(f"Starting nginx log processing...")
        print(f"Input path: {args.input_path}")
        
        # Setup Iceberg database and tables
        setup_iceberg_database(spark)
        
        # Process raw logs
        enriched_logs = process_raw_logs(spark, args.input_path)
        
        if enriched_logs is None or enriched_logs.count() == 0:
            print("No logs found to process. Exiting.")
            return
        
        # Store main logs
        store_main_logs(spark, enriched_logs)
        
        # Generate analytics
        generate_daily_analytics(spark)
        generate_weekly_analytics(spark)
        
        # Show summary
        show_summary_stats(spark)
        
        print(f"\n✅ Processing completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during processing: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()