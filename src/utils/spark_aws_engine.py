import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from src.utils.config_loader import load_pipeline_config
from src.utils.logger import get_logger

ENV_FILE = ".env"
logger = get_logger(__name__)

def create_spark_session(app_name: str = "UrbanDataPipeline") -> SparkSession:
    logger.info("Loading environment configurations from local storage...")
    load_dotenv(ENV_FILE)

    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not aws_access_key or not aws_secret_key:
        logger.error("AWS credentials extraction failed.")
        raise ValueError("AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY are missing.")
    
    config = load_pipeline_config()
    warehouse_dir = config['iceberg']['warehouse_dir']
    catalog_name = config['iceberg']['catalog_name']
    aws_region = config['aws']["region"]

    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2"
    ]
    package_string = ",".join(packages)

    logger.info("Initializing Spark session with hardened S3A configurations...")
    
    # We use explicit integer types and force cache disabling 
    # to completely ignore system-level configuration files (core-site.xml)
    # that are injecting the invalid '24h' string.
    spark_builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", package_string) \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.timeout", 1200000) \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", 120000) \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", 60) \
        .config("spark.hadoop.fs.s3a.multipart.size", 104857600) \
        .config("spark.hadoop.fs.s3a.multipart.threshold", 104857600) \
        .config("spark.hadoop.fs.s3a.attempts.maximum", 5) \
        .config("spark.hadoop.fs.s3a.paging.maximum", 1000) \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop") \
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_dir)

    try:
        spark = spark_builder.getOrCreate()
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.fatal(f"Fatal failure while instantiating spark session: {str(e)}")
        raise