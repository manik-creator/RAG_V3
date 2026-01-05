import json
import os
from src.workflow import MappingWorkflow
from dotenv import load_dotenv

load_dotenv()

def main():
    if not os.getenv("GROQ_API_KEY"):
        print("Error: GROQ_API_KEY not found in environment. Please set it in .env file.")
        return

    print("Starting Unified Cloud Asset Mapping Engine...")
    
    workflow = MappingWorkflow(data_dir="data")
    result = workflow.run()
    
    output_dir = "output"
    output_file = os.path.join(output_dir, "unified_mapping_recommendations.json")
    
    with open(output_file, 'w') as f:
        json.dump(result["final_output"], f, indent=2)
            
    print(f"Saved unified mapping recommendations to {output_file}")

    print("\nAll mappings generated and unified successfully!")

if __name__ == "__main__":
    main()
