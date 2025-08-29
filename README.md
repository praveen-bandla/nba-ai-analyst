# NBA AI Analyst

The NBA is a complex ecosystem — shaped by player and team statistics, contracts, trade assets, the CBA, awards, and more. It evolves constantly, yet there is no unified, context-driven engine to retrieve and analyze the latest information.
This gap motivates NBA AI Analyst: a LangGraph-based AI agent that integrates real-time statistics and league data to deliver accurate insights and computed analysis. For casual fans, it provides a reliable source of up-to-date information; for dedicated followers, it serves as a tool to explore and compute advanced analytics.

## High-Level Overivew

The analyst is available through a **Streamlit-based chatbot interface**, giving users a conversational front end. A **LangGraph agent** interprets each query and routes it either to a **retriever** (for fetching relevant information) or an **analyst** (for computing statistics from the dataset).  

Data is maintained in three formats: **raw CSVs** (collected via web scraping), **Parquet files** (optimized for fast access), and **vector stores** (indexed with FAISS for semantic retrieval). Each user interaction is logged as part of the chat session, and every query is evaluated in real time to generate the corresponding response.



## Data Assets

There are 5 kinds of datasets: 

1. [Player Contracts](data/raw_csv/player_contracts_with_notes.csv): Player salaries for the next few years, with notes on renewal dates and upcoming extensions
2. [Player Stats](data/raw_csv/all_player_stats_by_team.csv): Player total stats for the 2024-2025 season
3. [Team Cap Sheets](data/raw_csv/team_capsheets.csv): Teams capspace for the next 5 seasons
4. [Team Picks](data/raw_csv/nba_draft_picks_rag.csv): A list of all team picks including notes on the details of any protections that may apply
5. [Team Stats](data/raw_csv/total_team_stats.csv): Team total stats for the 2024-2025 seasons


## Repository Structure
```
config/
  settings.py                # Global configurations

data/
  parquet/                   # Parquet datasets used by agents
  raw_csv/                   # Raw + intermediate CSV sources

notebooks/                   # Notebooks used for webscrapping data
  webscrapping.ipynb         
  merge_payroll_notes_player_contracts.ipynb
  aggregate_player_stats_by_team.ipynb

src/
  agents/                    # Agents
  capabilities/              # Domain vocab / manifests
  embeddings/                # Scripts to build vector embeddings
  execution/                 # Execute SQL query
  graphs/                    # LangGraph / orchestration graph
  parquet_builders/          # Raw -> parquet transformation scripts
  tools/                     # Tools
  tests/                     # Test scripts

streamlit/
  app.py                     # UI entrypoint

vector_stores/               # FAISS indices + pickled metadata
```

## Configuration
Edit [config/settings.py](config/settings.py) for environment flags / paths.  
Environment variables loaded from [.env](.env) (if present).

## Installation
```
python -m venv .venv
source .venv/bin/activate  
pip install -r requirements.txt
```

## Agents & Orchestration


Core agents (see [src/agents](src/agents)):

- **[langgraph_agent.py](src/agents/langgraph_agent.py)**: Builds and configures the LangGraph topology (nodes + edges) that formalizes permissible execution transitions. It centralizes graph construction so the **orchestrator_agent** can execute a consistent state machine each turn.
- **[orchestrator_agent.py](src/agents/orchestrator_agent.py)**: Maintains shared conversational / working state and advances the graph step‑by‑step. It delegates concrete work to specialized agents and halts only when the **synthesis_agent** returns a finalized answer.
- **[analysis_agent.py](src/agents/analysis_agent.py)**: Calls the planner agent to execute a workflow for data computation. 
- **[planner_agent.py](src/agents/planner_agent.py)**: Interprets the user query and emits a structured, ordered task plan (datasets, metrics, transformations). Its output guides the **router_agent** and reduces unnecessary retrieval or computation.
- **[router_agent.py](src/agents/router_agent.py)**: Reads the evolving state (plan steps, fulfilled requirements, pending outputs) and decides whether the next hop is retrieval, analysis, or synthesis. It is the control switch that prevents premature summarization and ensures all planner tasks are satisfied.
- **[retrieval_agent.py](src/agents/retrieval_agent.py)**: Executes semantic lookups against FAISS vector stores plus any structured parquet access via registered retriever tools. It returns normalized document / row payloads so the **analysis_agent** can compute derived metrics without re‑querying sources.
d data and produces structured metric objects consumed by the **synthesis_agent**.
- **[synthesis_agent.py](src/agents/synthesis_agent.py)**: Consolidates retrieved facts and computed metrics into a coherent, context‑aware narrative answer. It references planner intent and includes reconciled insights while avoiding duplicate or conflicting statements.

Graph definition: [src/graphs/main_graph.py](src/graphs/main_graph.py)  
Execution harness: [src/execution/executor.py](src/execution/executor.py) -> Redirects the outputs of the router agent to the corresponding compute tool with any formatting changes necessary.


## Tool Layer

[Retriever Tools](src/tools/retriever): Based on the user query's contents, this folder contains 5 tools (one per dataset), that is tasked with retrieving the $k$ most relevant data entries for the corresponding vector store using FAISS.

 
[Compute Tools](src/tools/compute): Based on the **router agent**'s insights, these tools convert the operations to be executed from a json to a sql query. This step is purely deterministic, and involves no LLMs.



## Vector Embeddings
[Embedding Scripts](src/embeddings): Each script (e.g. [player_contracts.py](src/embeddings/player_contracts.py), [team_stats.py](src/embeddings/team_stats.py)) loads its parquet dataset, normalizes / flattens relevant fields, and converts each row (or logical group of rows) into a semantically rich text chunk. These chunks are embedded (LLM embedding model configured in code / settings) and written into a FAISS index alongside a Python pickle storing auxiliary metadata (row ids, dataset type, lightweight schema hints).


## Streamlit App
Run UI:
```
streamlit run streamlit/app.py
```

Behavior:
- Maintains session state (conversation history + intermediate structured outputs)
- Dispatches each new prompt to the orchestrator which triggers planner → router → (retrieval / analysis loops) → synthesis
- Can be refreshed safely after rebuilding embeddings (state will reinitialize unless you persist it externally)
- Contains some sample requests provided to the user for reference



## Limitations and Challenges

1. **Query Complexity**  
   The agent currently supports relatively simple aggregations (e.g., maximum, minimum, average, median). It cannot yet handle more complex operations such as nested queries, multi-table joins, or advanced analytical pipelines.

2. **Nomenclature and Aliases**  
   Player and team aliases remain a challenge. While common nicknames (e.g., *KD* for Kevin Durant, *AD* for Anthony Davis) are included in the manifest, coverage is incomplete. This limits contextual understanding when users refer to less common abbreviations or alternative names.

3. **Semi-Generative Components**  
   Although LLMs are used for most generative tasks (e.g., roadmap creation), some functionality has been simplified into classification. For instance, available metrics and certain aggregation options are predefined and surfaced for the model to select from, rather than being dynamically generated at runtime.

4. **Markdown Rendering Issues**  
   Since LLMs generate markdown-formatted responses, hallucinations or formatting errors occasionally occur. This can lead to broken rendering or unreadable text in the chat interface.

5. **Data Constraints**
   - **Limited sample window**: Current statistics only cover the 2024–2025 season.  
   - **Split-year data**: Players traded mid-season (e.g., Anthony Davis) have fragmented records that do not fully represent their year-round performance.  
   - **Contract listings**: For players with future contracts (e.g., Paolo Banchero’s rookie max extension), only total values are captured—year-to-year salary progression is missing.

6. **Lack of modularization of webscrapping**: The data gathering process was treated as a one-time set-up process and was not set up with modularability for updating the dataset.


## Expansions

- **New datasets**: Extend coverage by adding a Parquet builder for preprocessing, an embedding script for vectorization, and a retriever tool. Each new tool must be registered in [`src/tools/tool_registry.py`](src/tools/tool_registry.py).  
- **New analytic capabilities**: Implement additional compute tools under [`src/tools/compute`](src/tools/compute), such as advanced player metrics, lineup analysis, or trade simulators. Expose these through the planner or synthesis agents.  
- **Historical data integration**: Incorporate prior seasons into the pipeline to enable trend analysis, comparisons, and long-term player/team evaluations.  
- **Real-time updates**: Add support for periodically ingesting live stats, transactions, and news feeds to keep the system current.  
- **Advanced retrieval**: Support hybrid search (keyword + vector) and cross-dataset joins for richer query responses.  
- **Visualization outputs**: Extend response generation with charts, tables, or interactive plots for more intuitive insights.  
- **User customization**: Allow users to define favorite players, teams, or metrics and bias retrieval/analysis toward those preferences.  
- **Multi-agent workflows**: Introduce specialized agents (e.g., contracts analyst, trade evaluator) orchestrated by LangGraph for modular expansion.

## Improvements

- **Schema validation**: Introduce stricter Pydantic models to enforce argument structure and prevent malformed queries.  
- **Testing**: Expand the unit and integration test suite to cover each tool and agent for robustness.  
- **CLI utilities**: Add command-line interfaces for tasks like dataset rebuilds and index refreshes.  
- **Incremental updates**: Support partial index and embedding refreshes to avoid full rebuilds when new data is added.  
- **Caching**: Implement a caching layer to improve performance on frequent or repeated queries.  
- **Monitoring & logging**: Standardize logging configuration and add monitoring hooks for query traces, latency, and errors.

