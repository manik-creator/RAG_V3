import json
import os
import chromadb
from chromadb.utils import embedding_functions
from typing import List, Dict, Any

class AssetIngestor:
    def __init__(self, db_path: str = "./chroma_db", collection_name: str = "cloud_assets"):
        self.client = chromadb.PersistentClient(path=db_path)
        # Using a simple default embedding function (sentence-transformers)
        self.embedding_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name="all-MiniLM-L6-v2"
        )
        self.collection = self.client.get_or_create_collection(
            name=collection_name, 
            embedding_function=self.embedding_fn
        )

    def parse_jsonl(self, file_path: str) -> List[Dict[str, Any]]:
        """Parses a .jsonl file and returns a list of dictionaries."""
        records = []
        with open(file_path, 'r') as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
        return records

    def ingest_file(self, file_path: str, default_provider: str = None, default_asset: str = None):
        """Processes a .jsonl file. Records can be mixed; extracts metadata from each."""
        records = self.parse_jsonl(file_path)
        
        ids = []
        documents = []
        metadatas = []
        
        for i, record in enumerate(records):
            # Try to extract metadata from record, otherwise use defaults
            # Orca context: cloud_provider, type
            provider = record.get("cloud_provider") or record.get("cloudContext", {}).get("cloudType") or default_provider or "unknown"
            asset = record.get("asset_type") or record.get("type") or default_asset or "unknown"
            
            # Standardize provider
            provider = provider.lower()
            if "aws" in provider: provider = "aws"
            elif "azure" in provider: provider = "azure"
            elif "gcp" in provider or "google" in provider: provider = "gcp"

            doc_str = json.dumps(record)
            record_id = record.get("id") or record.get("InstanceId") or f"{provider}_{asset}_{i}_{os.path.basename(file_path)}"
            
            ids.append(str(record_id))
            documents.append(doc_str)
            metadatas.append({
                "cloud_provider": provider,
                "asset_type": asset,
                "source_file": os.path.basename(file_path)
            })
            
        if ids:
            self.collection.upsert(
                ids=ids,
                documents=documents,
                metadatas=metadatas
            )
            print(f"Ingested {len(ids)} records from {file_path}")

    def get_sample_records(self, cloud_provider: str, asset_type: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Retrieves sample records for a specific provider and asset type."""
        results = self.collection.get(
            where={
                "$and": [
                    {"cloud_provider": cloud_provider},
                    {"asset_type": asset_type}
                ]
            },
            limit=limit
        )
        
        return [json.loads(doc) for doc in results['documents']]

    def get_unique_pairs(self) -> List[Dict[str, str]]:
        """Identifies all unique combinations of asset_type + cloud_provider from the vector store."""
        # We fetch all metadatas to find unique pairs
        # Note: For very large datasets, this should be optimized
        results = self.collection.get(include=["metadatas"])
        metadatas = results.get("metadatas", [])
        
        unique_pairs = set()
        for meta in metadatas:
            unique_pairs.add((meta["cloud_provider"], meta["asset_type"]))
            
        return [{"provider": p, "asset": a} for p, a in unique_pairs]

if __name__ == "__main__":
    # Quick test
    ingestor = AssetIngestor()
    print("Ingestor initialized.")
