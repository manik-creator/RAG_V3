import json
import os
import pandas as pd
from typing import List, Dict, Any

class DataTransformer:
    def __init__(self, mapping_path: str = "output/unified_mapping_recommendations.json"):
        self.mapping_path = mapping_path
        self.mappings = self._load_and_invert_mappings()

    def _load_and_invert_mappings(self) -> Dict[str, Dict[str, str]]:
        """
        Transforms the output format into a lookup:
        { "aws_ec2": { "ID": "InstanceId", "Name": "Tags.Name" } }
        """
        if not os.path.exists(self.mapping_path):
            print(f"Warning: Mapping file {self.mapping_path} not found.")
            return {}

        with open(self.mapping_path, 'r') as f:
            unified_map = json.load(f)

        inverted = {}
        for target_field, sources in unified_map.items():
            for s_entry in sources:
                source_key = s_entry["source"]
                if source_key not in inverted:
                    inverted[source_key] = {}
                inverted[source_key][target_field] = s_entry["mapping_field"]
        
        return inverted

    def get_nested_value(self, data: Dict[str, Any], path: str) -> Any:
        """Handles extraction of values from nested keys using dot notation."""
        if not path:
            return None
        
        parts = path.split(".")
        val = data
        try:
            for part in parts:
                if isinstance(val, dict):
                    val = val.get(part)
                else:
                    return None
            return val
        except Exception:
            return None

    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Applies mappings to a single record."""
        meta = record.get("__metadata__", {})
        source_key = f"{meta.get('cloud_provider')}_{meta.get('asset_type')}"
        
        mapping = self.mappings.get(source_key, {})
        unified_record = {}
        
        # We want the CSV to have all fields from the target schema (conceptually)
        # For now, we use the fields present in the mapping results
        for target_field, source_path in mapping.items():
            unified_record[target_field] = self.get_nested_value(record, source_path)
            
        return unified_record

    def transform_all(self, records: List[Dict[str, Any]]) -> pd.DataFrame:
        """Transforms a list of raw records into a unified DataFrame."""
        transformed = []
        for record in records:
            transformed_rec = self.transform_record(record)
            if transformed_rec:
                transformed.append(transformed_rec)
        
        return pd.DataFrame(transformed)

if __name__ == "__main__":
    # Test path resolution
    transformer = DataTransformer()
    print("Transformer initialized.")
