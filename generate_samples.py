import json
import random

def generate_samples(count=1000):
    assets = [
        {"provider": "aws", "type": "ec2_instance", "fields": ["InstanceId", "PublicIpAddress", "PrivateDnsName"]},
        {"provider": "aws", "type": "s3_bucket", "fields": ["BucketName", "CreationDate", "Region"]},
        {"provider": "azure", "type": "virtual_machine", "fields": ["id", "name", "location"]},
        {"provider": "azure", "type": "storage_account", "fields": ["id", "name", "kind"]},
        {"provider": "gcp", "type": "compute_instance", "fields": ["id", "name", "zone"]},
        {"provider": "gcp", "type": "storage_bucket", "fields": ["id", "name", "location"]}
    ]
    
    samples = []
    for i in range(count):
        asset_info = random.choice(assets)
        record = {
            "id": f"id-{i}",
            "cloud_provider": asset_info["provider"],
            "type": asset_info["type"],
            "name": f"resource-{i}"
        }
        
        # Add provider-specific noise/fields
        for field in asset_info["fields"]:
            record[field] = f"value-{field}-{i}"
            
        if asset_info["provider"] == "aws":
            record["Tags"] = {"Name": f"aws-resource-{i}", "ENV": "prod"}
            record["cloudContext"] = {"cloudType": "AWS"}
        elif asset_info["provider"] == "azure":
            record["properties"] = {"vmId": f"az-id-{i}", "hardwareProfile": {"vmSize": "Standard_D2"}}
        
        samples.append(record)
    
    with open("data/mixed_assets.jsonl", "w") as f:
        for s in samples:
            f.write(json.dumps(s) + "\n")
            
    print(f"Generated {count} samples in data/mixed_assets.jsonl")

if __name__ == "__main__":
    generate_samples(1000)
