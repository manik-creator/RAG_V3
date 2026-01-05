# Unified Cloud Asset Mapping Engine

This project builds an automated pipeline that ingests heterogeneous cloud asset data, stores it in ChromaDB, and uses a LangGraph-powered agent with Groq LLM to generate semantic mappings to a unified global schema.

## Features
- **Data Ingestion**: Parses `.jsonl` files and stores them in ChromaDB with metadata.
- **RAG-based Sampling**: Retrieves representative samples for each cloud provider and asset type.
- **LangGraph Orchestration**: Uses a state machine to discover, retrieve, and map assets sequentially.
- **LLM Reasoning**: Leverages Groq (Llama 3/Mixtral) to analyze source fields and map them to a target unified schema.
- **Unified Schema**: Standardizes fields like `ID`, `Name`, `Host_name`, `IP_Address`, `Memory`, etc.

## Setup

1. **Install Dependencies**:
   Using `uv`:
   ```bash
   uv pip install -r requirements.txt
   ```
   *Alternative*: If you initialize a uv project:
   ```bash
   uv add langgraph chromadb groq langchain-groq langchain-community langchain sentence-transformers python-dotenv pandas
   ```

2. **Configure Environment**:
   - Rename `.env.template` to `.env`.
   - Add your `GROQ_API_KEY`.

3. **Data Placement**:
   - Place your Orca Cloud asset exports (`.jsonl`) in the `data/` directory.
   - naming convention: `<cloud_provider>_<asset_type>.jsonl` (e.g., `aws_ec2.jsonl`).

## Usage

Run the pipeline:
```bash
uv run main.py
```

The generated mappings will be saved in the `output/` directory as JSON files.

## Project Structure
- `src/ingest.py`: ChromaDB ingestion logic.
- `src/llm.py`: Groq integration and mapping prompt.
- `src/workflow.py`: LangGraph state machine.
- `src/schema.py`: Target Unified Asset Schema.
- `main.py`: Entry point.
