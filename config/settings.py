'''
All settings, configurations, and constants for the NBA AI Analyst.
'''

import os

# models
EMBEDDING_MODEL = "text-embedding-3-small"
PLANNER_AGENT = "gpt-4o-mini"
RETRIEVAL_AGENT = "gpt-4o-mini"
ROUTER_AGENT = "gpt-4o-mini"
SYNTHESIS_AGENT = "gpt-4o-mini"
ORCHESTRATOR_AGENT = "gpt-4o-mini"

# Data paths
DATA_DIR = "data"

RAW_DATA_DIR = os.path.join(DATA_DIR, "raw_csv")
DATASETS = {
    "player_contracts": os.path.join(RAW_DATA_DIR, "player_contracts_with_notes.csv"),
    "player_stats": os.path.join(RAW_DATA_DIR, "all_player_stats_by_team.csv"),
    "team_capsheets": os.path.join(RAW_DATA_DIR, "team_capsheets.csv"),
    "team_picks": os.path.join(RAW_DATA_DIR, "nba_draft_picks_rag.csv"),
    "team_stats": os.path.join(RAW_DATA_DIR, "total_team_stats.csv")
}

PARQUET_DATA_DIR = os.path.join(DATA_DIR, "parquet")
PARQUET_FOLDERS = {
    "player_contracts": PARQUET_DATA_DIR,
    "player_stats": PARQUET_DATA_DIR,
    "team_capsheets": PARQUET_DATA_DIR,
    "team_picks": PARQUET_DATA_DIR,
    "team_stats": PARQUET_DATA_DIR
}

# Vectorstore output directories
VECTORSTORE_DIR = "vector_stores"
INDEX_PATHS = {
    "player_contracts": os.path.join(VECTORSTORE_DIR, "player_contracts_index"),
    "player_stats": os.path.join(VECTORSTORE_DIR, "player_stats_index"),
    "team_capsheets": os.path.join(VECTORSTORE_DIR, "team_capsheets_index"),
    "team_picks": os.path.join(VECTORSTORE_DIR, "team_picks_index"),
    "team_stats": os.path.join(VECTORSTORE_DIR, "team_stats_index")
}
