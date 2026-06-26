import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from src.utils.config_loader import load_pipeline_config
from src.utils.logger import get_logger

ENV_FILE = ".env"

logger = get_logger(__name__)

def create_spark_session(app_name: str = "UrbanDataPipeline") -> SparkSession:
    
    logger.info("Loading environment configurations from local storage...")
    load_dotenv(ENV_FILE); # loads credentials

    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not aws_access_key or not aws_secret_key:
        logger.error("AWS credentials extraction failed.")
        raise ValueError("AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY are misssing.")
    
    logger.info(f"AWS credentials extracted successfully from {ENV_FILE}")

    logger.info("Parsing pipeline_config.yaml structural layout...")
    try:
        config = load_pipeline_config()
        warehouse_dir = config['iceberg']['warehouse_dir']
        catalog_name = config['iceberg']['catalog_name']
        aws_region = config['aws']["region"]
        logger.info(f"Configuration succesfully parsed. Active catalog: {catalog_name}")
    except KeyError as e:
        logger.error(f"Missing mandatory configuration block within YAML schema: {str(e)}")
        raise

    logger.info("Preapre Maven runtime packages for Iceberg + AWS...")
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2"
    ]
    package_string = ",".join(packages)

    logger.info("Start local JVM and initialize pyspark cluster session...")
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", package_string) \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop") \
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_dir) \
            .getOrCreate()
        
        logger.info("Pyspark runtime environment successfully created.")
        logger.info(f"Iceberg storage engine active on warehouse: {warehouse_dir}")

        return spark
    
    except Exception as e:
        logger.fatal(f"Fatal failure while instantiating spark session: {str(e)}")
        raise