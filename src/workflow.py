import os
import glob
from typing import TypedDict, List, Dict, Any
from langgraph.graph import StateGraph, END
from src.ingest import AssetIngestor
from src.llm import LLMHandler

# Define the state structure
class GraphState(TypedDict):
    pairs: List[Dict[str, str]]  # List of {"provider": ..., "asset": ...}
    current_index: int
    current_pair: Dict[str, str]
    samples: List[Dict[str, Any]]
    mappings: List[Dict[str, Any]]
    final_output: Dict[str, Any]

class MappingWorkflow:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.ingestor = AssetIngestor()
        self.llm_handler = LLMHandler()
        self.workflow = self._create_workflow()

    def _create_workflow(self):
        workflow = StateGraph(GraphState)

        # Define Nodes
        workflow.add_node("discovery", self.node_discovery)
        workflow.add_node("retrieval", self.node_retrieval)
        workflow.add_node("mapping", self.node_mapping)
        workflow.add_node("aggregator", self.node_aggregator)

        # Define Edges
        workflow.set_entry_point("discovery")
        workflow.add_edge("discovery", "retrieval")
        workflow.add_edge("retrieval", "mapping")
        
        # Conditional edge to loop or finish
        workflow.add_conditional_edges(
            "mapping",
            self.should_continue,
            {
                "continue": "retrieval",
                "end": "aggregator"
            }
        )
        
        workflow.add_edge("aggregator", END)

        return workflow.compile()

    # --- Nodes ---

    def node_discovery(self, state: GraphState):
        """Identifies all unique combinations of asset_type + cloud_provider from the vector store after ingestion."""
        print("--- Node: Discovery ---")
        files = glob.glob(f"{self.data_dir}/*.jsonl")
        
        for file in files:
            filename = os.path.basename(file)
            # Try to guess provider/asset from filename as backup
            parts = filename.replace(".jsonl", "").split("_", 1)
            default_provider, default_asset = (parts[0], parts[1]) if len(parts) == 2 else (None, None)
            
            # Ingest file (smarter extraction happens inside)
            self.ingestor.ingest_file(file, default_provider, default_asset)
        
        # Get unique pairs from what was actually ingested
        pairs = self.ingestor.get_unique_pairs()
        print(f"Discovered {len(pairs)} unique asset/provider combinations.")
        
        return {
            "pairs": pairs,
            "current_index": 0,
            "mappings": []
        }

    def node_retrieval(self, state: GraphState):
        """Retrieves sample records for the current pair."""
        print(f"--- Node: Retrieval (Index {state['current_index']}) ---")
        current_pair = state["pairs"][state["current_index"]]
        samples = self.ingestor.get_sample_records(
            current_pair["provider"], 
            current_pair["asset"]
        )
        return {
            "current_pair": current_pair,
            "samples": samples
        }

    def node_mapping(self, state: GraphState):
        """Calls LLM to generate mapping."""
        print(f"--- Node: Mapping ({state['current_pair']['asset']} on {state['current_pair']['provider']}) ---")
        mapping = self.llm_handler.generate_mapping(
            state["current_pair"]["provider"],
            state["current_pair"]["asset"],
            state["samples"]
        )
        
        new_mappings = state["mappings"] + [mapping]
        return {
            "mappings": new_mappings,
            "current_index": state["current_index"] + 1
        }

    def node_aggregator(self, state: GraphState):
        """Finalizes the mappings into a unified recommendation format with deduplication."""
        print("--- Node: Aggregator ---")
        
        unified_recommendation = {}
        # Track seen pairs to avoid duplicates: {target_field: set((source, mapping_field))}
        seen_recommendations = {}
        
        for entry in state["mappings"]:
            source = f"{entry['cloud_provider']}_{entry['source_type']}"
            mappings = entry.get("mappings", {})
            
            for source_field, target_field in mappings.items():
                if target_field not in unified_recommendation:
                    unified_recommendation[target_field] = []
                    seen_recommendations[target_field] = set()
                
                recommendation_pair = (source, source_field)
                if recommendation_pair not in seen_recommendations[target_field]:
                    unified_recommendation[target_field].append({
                        "source": source,
                        "mapping_field": source_field
                    })
                    seen_recommendations[target_field].add(recommendation_pair)
        
        return {"final_output": unified_recommendation}

    # --- Edges ---

    def should_continue(self, state: GraphState):
        if state["current_index"] < len(state["pairs"]):
            return "continue"
        return "end"

    def run(self):
        # Initialize state
        initial_state = {
            "pairs": [],
            "current_index": 0,
            "mappings": [],
            "final_output": {}
        }
        return self.workflow.invoke(initial_state)

if __name__ == "__main__":
    # Ensure directories exist
    os.makedirs("data", exist_ok=True)
    os.makedirs("output", exist_ok=True)
    
    # workflow = MappingWorkflow()
    # result = workflow.run()
    # print(result["final_output"])
