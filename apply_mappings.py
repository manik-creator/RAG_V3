import os
from src.ingest import AssetIngestor
from src.transform import DataTransformer

def main():
    print("Starting Mapping Application Job...")
    
    # 1. Initialize Ingestor and retrieve all records
    ingestor = AssetIngestor()
    records = ingestor.get_all_records()
    print(f"Retrieved {len(records)} records from ChromaDB.")
    
    if not records:
        print("No records found in database. Please run the mapping pipeline first.")
        return

    # 2. Initialize Transformer
    transformer = DataTransformer(mapping_path="output/unified_mapping_recommendations.json")
    
    # 3. Transform records
    print("Applying mappings and transforming data...")
    df = transformer.transform_all(records)
    
    # 4. Export to CSV
    output_path = "output/unified_assets.csv"
    os.makedirs("output", exist_ok=True)
    df.to_csv(output_path, index=False)
    
    print(f"\nTransformation complete!")
    print(f"Unified assets exported to: {output_path}")
    print(f"Total columns: {len(df.columns)}")
    print(f"Total rows: {len(df)}")

if __name__ == "__main__":
    main()
