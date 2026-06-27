import os
import numpy as np
import pandas as pd
from scipy.spatial.distance import cosine
from dotenv import load_dotenv
from transformers import AutoTokenizer, AutoModel
import torch

from src.utils.config_loader import load_pipeline_config
from src.utils.logger import get_logger

logger = get_logger(__name__)

class UrbanKnowledgeRetriever:
    def __init__(self) -> None:

        logger.info("Booting up the local LLM vector search engine...")
        load_dotenv()

        self.config = load_pipeline_config()
        self.gold_path = self.config['s3_paths']['gold'] + "llm_knowledge_base"

        self.model_name = self.config['llm_enrichment']['embedding_model']
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModel.from_pretrained(self.model_name)
        self.model.eval()

        logger.info(f"Loading gold knowledge base vectors from S3: {self.gold_path}")

        # replace 's3a' with 's3' as pandas prefers using 's3'
        pandas_s3_path = self.gold_path.replace("s3a://", "s3://")
        self.knowledge_base = pd.read_parquet(pandas_s3_path)
        logger.info(f"Loaded {len(self.knowledge_base)} vectors into memory")

    def embed_query(self, query_text: str) -> np.ndarray:

        inputs = self.tokenizer(
            query_text,
            padding=True,
            truncation=True,
            max_length=512,
            return_tensors="pt"
        )

        with torch.no_grad():
            outputs = self.model(**inputs)
            attention_mask = inputs['attention_mask']
            token_embeddings = outputs[0]
            input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
            sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)
            sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
            embedding = (sum_embeddings / sum_mask).squeeze(0).numpy()

        return embedding
    
    def search_and_explain(self, user_query: str, top_k: int = 1):

        logger.info(f"Processing user query: '{user_query}'")

        query_vector = self.embed_query(user_query) 

        # calculate cosine similarity distance for all rows
        self.knowledge_base['distance'] = self.knowledge_base['vector_embedding'].apply(
            lambda x: cosine(query_vector, np.array(x))
        )

        # sort to find the closest semantic matches
        top_matches = self.knowledge_base.sort_values(by='distance', ascending=True).head(top_k)

        logger.info("Search complete. Generating explainable LLM context... ")

        print(f"User prompt: {user_query}\n")
        for index, row in top_matches.iterrows():
            print(f"- {row['sector_narrative']}")

if __name__ == "__main__":
    retriever = UrbanKnowledgeRetriever()

    retriever.search_and_explain(
        user_query="Are there safe, well-lit cycleways in the northern sector?", 
        top_k=1
    )