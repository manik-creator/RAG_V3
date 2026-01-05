import os
import json
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from dotenv import load_dotenv
import json

load_dotenv()

class LLMHandler:
    def __init__(self, model_name: str = "meta-llama/llama-4-maverick-17b-128e-instruct"):
        self.llm = ChatGroq(
            api_key=os.getenv("GROQ_API_KEY"),
            model_name=model_name,
            temperature=0
        )
        self.parser = JsonOutputParser()

    def get_mapping_prompt(self):
        system_prompt = """
        You are a Cloud Infrastructure Schema Mapping Specialist.
        Your task is to analyze a sample of raw JSON records from a cloud provider and map their source fields to a Unified Asset Schema.

        ### Unified Asset Schema Fields:
        - ID: Unique identifier for the asset
        - Name: Display name of the asset
        - Host_name: Host name or DNS name
        - IP_Address: Primary IP address
        - Memory: Memory capacity or Type
        - Cloud_Provider: Cloud provider (aws, azure, gcp, etc.)
        - Cloud_Meta_Instance_ID: Provider-specific instance ID or availability zone
        - Cloud_Account_ID: Identifier for the cloud account/subscription

        ### Instructions:
        1. Examine THE JSON samples carefully.
        2. Identify which source keys correspond to the target fields.
        3. For nested fields, use dot notation (e.g., "Tags.Name").
        4. If a field is not present or cannot be determined, omit it from the mapping.
        5. RETURN ONLY a single JSON dictionary with the mapping logic. 
        6. NO conversational text, NO explanations, NO preamble or postamble.
        7. Ensure all keys in the "mappings" block are UNIQUE.

        ### Output Format:
        ```json
        {{
          "source_type": "<source_asset_type>",
          "cloud_provider": "<provider>",
          "mappings": {{
            "source_field_1": "Target_Field_1",
            "source_field_2": "Target_Field_2"
          }}
        }}
        ```
        """
        
        user_prompt = """
        Analyze these sample records for {asset_type} on {cloud_provider}:
        {samples}
        
        Generate the mapping JSON.
        """
        
        return ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            ("human", user_prompt)
        ])

    def generate_mapping(self, cloud_provider: str, asset_type: str, samples: list):
        prompt = self.get_mapping_prompt()
        chain = prompt | self.llm | self.parser
        
        # Ensure samples are clean JSON strings for the prompt
        samples_json = json.dumps(samples, indent=2)
        
        return chain.invoke({
            "cloud_provider": cloud_provider,
            "asset_type": asset_type,
            "samples": samples_json
        })
