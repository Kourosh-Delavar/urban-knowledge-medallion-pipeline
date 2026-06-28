# Urban Knowledge Medallion Pipeline

## Description

The Urban Knowledge Medallion Pipeline is a distributed ETL framework designed to transform raw urban spatial data into actionable intelligence. It implements a medallion architecture (Bronze, Silver, Gold) to process multi-modal data, perform advanced spatial indexing via Uber H3, and synthesize vector embeddings for Retrieval-Augmented Generation (RAG) systems.

The pipeline extracts urban perception data and network features from AWS S3, standardizes spatial representations, enriches data with semantic context, and provides a lineage-tracked knowledge base for LLM-based urban analytics.

## Architecture and Workflow

* **Bronze Layer (Ingestion):** Raw data extraction from S3 data lake, serving as the source of truth for all downstream transformations.

* **Silver Layer (`src/silver/h3_spatial_builder.py`):**

  * Standardizes geospatial attributes (Latitude/Longitude).

  * Applies Uber H3 spatial indexing at a configurable resolution for efficient spatial aggregation.

  * Loads data into an Apache Iceberg transactional table for robust schema evolution and snapshot management.

* **Gold Layer (`src/gold/llm_context_graph.py`):**

  * Synthesizes spatial data into descriptive natural language narratives.

  * Computes high-dimensional vector embeddings using transformer models (e.g., BGE).

  * Exports a vector knowledge base optimized for semantic vector similarity search.

* **Explainability & Retrieval (`src/utils/explainability_query.py`):**

  * Provides tools for querying the Gold layer via natural language.

  * Includes an automated lineage trace to link LLM outputs directly back to their source Iceberg snapshots and Bronze partitions.

## Prerequisites

To run this pipeline locally or on a cloud cluster, the following dependencies are required:

* **Python 3.10+**

* **OpenJDK 17** (Required for compatibility with Iceberg and modern Spark Hadoop connectors).

* **AWS IAM Credentials:** Configured with read/write access to the target S3 buckets.

* **Environment Variables:** An `.env` file containing `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

## Project Structure

```text
urban-knowledge-medallion-pipeline/
├── config/
│   └── pipeline_config.yaml      # Environment and pipeline parameters
├── scripts/
│   └── check_spark_env.py        # Diagnostic tool for Hadoop S3A config
├── src/
│   ├── gold/
│   │   └── llm_context_graph.py  # Embedding generation and Gold layer logic
│   ├── silver/
│   │   └── h3_spatial_builder.py # Spatial indexing and Iceberg transformation
│   └── utils/
│       ├── config_loader.py      # YAML configuration helper
│       ├── explainability_query.py # Vector search and lineage trace
│       ├── logger.py             # Standardized logging setup
│       └── spark_aws_engine.py   # Hardened Spark/S3A session factory
├── main.py                       # Pipeline execution orchestrator
└── README.md                     # Project documentation