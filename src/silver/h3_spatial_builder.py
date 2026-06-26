import sys
from pyspark.sql import SparkSession
from pyspark.sql.function import udf, col
from pyspark.sql.types import StringType

from src.utils.spark_aws_engine import create_spark_session
from src.utils.config_loader import load_pipeline_config
from src.utils.logger import get_logger

import h3

logger = get_logger(__name__)

def convert_lat_lon_to_h3(lat: float, lon: float, resolution: int) -> str:

    if lat is None or lon is None:
        return None
    try:
        # h3.geo_to_h3 expects (latitude, longitude, resolution)
        return h3.geo_to_h3(lat, lon, resolution)
    except Exception:
        # return None for corrupted geographic points to keep the pipeline from crashing 
        return None 
    
def build_silver_layer():
    logger.info("Initializing silver layer processing pipeline...")

    # load project configuration variables
    config = load_pipeline_config()
    bronze_path = config['s3_paths']['bronze']
    h3_res = config['spatial_processing']['h3_resolution']
    catalog_name = config['iceberg']['catalog_name']

    # define the Iceberg table name pattern
    silver_table_identifier = f"{catalog_name}.silver.deventer_spatial"

    spark = create_spark_session("SilverSpatialBuilder")

    try:
        logger.info(f"Extracting source parquet files from bronze bucket path: {bronze_path}")

        bronze_df = spark.read.parquet(bronze_path)

        row_count = bronze_df.count()
        logger.info(f"Successfully extracted {row_count} records from bronze layer.")

        if row_count == 0:
            logger.warning("Bronze layer is empty.")
            return
        
        # register out h3 conversion function as a pyspark UDF
        logger.info(f"Register distributed Uber H3 udf at resolution: {h3_res}")
        h3_udf = udf(lambda lat, lon: convert_lat_lon_to_h3(lat, lon, h3_res), StringType())

        logger.info("Begin distributed dataframe transformations...")

        lat_col = "latitude" if "latitude" in bronze_df.columns else "lat"
        lon_col = "longitude" if "longitude" in bronze_df.columns else "lon"

        if lat_col not in bronze_df.columns or lon_col not in bronze_df.columns:
            logger.error(f"Geospatial columns not found in bronze schema. Availabel: {bronze_df.columns}")
            raise KeyError("Mandatory coordinate columns lat & lon missing from the source.")

        # apply the udf to create a new column named 'h3_index'
        silver_df = bronze_df.withColumn("h3_index", h3_udf(col(lat_col), col(lon_col), ))

        # filter out rows where spatial indexing failed (corrupted or outside bound data)
        silver_df = silver_df.filter(col("h3_index")).isNotNull()

        logger.info(f"Wrtie enriched data to Iceberg silver table target: {silver_table_identifier}")

        # ensure the Iceberg silver database namespace exists inside our catalog context
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.silver")

        silver_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .partitionBy("highway") \
            .save(silver_table_identifier)
        
        logger.info(f"Silver layer table updates completed successfully inside Iceberg format.")
    
    except Exception as e:
        logger.fatal(f"Silver layer execution halted: {str(e)}")
        sys.exit(1)
    finally:
        logger.info("Terminate the Spark")
        spark.stop()

if __name__ == "__main__":
    build_silver_layer()