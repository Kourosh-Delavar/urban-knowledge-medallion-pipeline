from typing import Any
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, 
    concat_ws,
    lit,
    collect_list,
    pandas_udf
)
from pyspark.sql.types import ArrayType, FloatType
import pandas as pd

from src.utils.spark_aws_engine import create_spark_session
from src.utils.config_loader import load_pipeline_config
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Global variable to cache the model on spark executers (avoid reloading for every batch)
_model_cache = None

def get_embedding_model():
    
    global _model_cache
    if _model_cache is None:
        from transformers import AutoTokenizer, AutoModel
        import torch

        config = load_pipeline_config()
        model_name = config['llm_enrichment']['embedding_model']
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        model = AutoModel.from_pretrained(model_name)
        model.eval() # set model to evaluation mode
        _model_cache = (tokenizer, model)
    
    return _model_cache

@pandas_udf(ArrayType(FloatType()))
def compute_local_embeddings_udf(texts: pd.Series) -> Any:

    import torch 
    tokenizer, model = get_embedding_model()

    embeddings_list = []

    for text in texts:
        if not text:
            embeddings_list.append([0.0] * 384) # fallback dimension for bge-small
            continue

        inputs = tokenizer(
            text,
            padding=True,
            truncation=True,
            max_length=512,
            return_tensors="pt"
        )

        with torch.no_grad():
            model_output = model(**inputs)
            # perform mean pooling to get a single 384-dimensional vector
            attention_mask = inputs['attention_mask']
            token_embeddings = model_output[0]
            input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
            sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)
            sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
            embedding = (sum_embeddings / sum_mask).squeeze(0).tolist()

        embeddings_list.append(embedding)

    return pd.Series(embeddings_list)

def build_gold_layer():
    
    logger.info("Initializing gold layer pipeline...")

    config = load_pipeline_config()
    catalog_name = config['iceberg']['catalog_name']
    gold_path = config['s3_paths']['gold']

    silver_table_identifier = f"{catalog_name}.silver.deventer_spatial"
    gold_target_path = f"{gold_path}llm_knowledge_base/"

    spark = create_spark_session("GoldLLMContextBuilder")

    try:
        logger.info(f"Reading from transactional Iceberg silver table: {silver_table_identifier}")
        silver_df = spark.read.format("iceberg").load(silver_table_identifier)

        if silver_df.rdd.isEmpty():
            logger.warning("No records found in silver table. Aborting gold build.")
            return
        
        excluded_cols = {"latitude", "longitude", "lat", "lon", "h3_index"}
        perception_cols = [c for c in silver_df.columns if c  not in excluded_cols]

        logger.info(f"Targeting attributes for semantic processing: {perception_cols}")

        text_df = silver_df.withColumn(
            "descriptive_text",
            concat_ws(
                " ",
                lit("Within geographic sector code"), col("h3_index"),
                lit("the infrastructure classification is a"), col("highway"),
                lit(". Descriptive metadata indicates:"), 
                concat_ws(", ", *[col(c) for c in perception_cols if c != "highway"])
            )
        )

        logger.info("Aggregating sector summaries by uniform spatial hex grids...")
        gold_base_df = text_df.groupBy("h3_index") \
            .agg(concat_ws(" Additionally, ", collect_list("descriptive_text",)).alias("sector_narrative"))

        logger.info("Execute vector embedding calculation...")
        gold_vector_df = gold_base_df.withColumn(
            "vector_embedding",
            compute_local_embeddings_udf(col("sector_narrative"))
        )

        logger.info("Gold layer successfully built. Urban data is fully optimized for for RAG LLM systems.")
    
    except Exception as e:
        logger.fatal(f"Gold layer creation failed: {str(e)}")
        sys.exit(1)
    finally:
        logger.info("Terminate the Spark")
        spark.stop()

if __name__ == "__main__":
    build_gold_layer()