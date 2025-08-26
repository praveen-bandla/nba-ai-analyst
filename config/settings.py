import os

# Embedding model
EMBEDDING_MODEL = "text-embedding-3-small"

# Data paths
DATA_DIR = "data"
DATASETS = {
    "player_contracts": os.path.join(DATA_DIR, "player_contracts_with_notes.csv"),
    "player_stats": os.path.join(DATA_DIR, "all_player_stats_by_team.csv"),
    "team_capsheets": os.path.join(DATA_DIR, "team_capsheets.csv"),
    "team_picks": os.path.join(DATA_DIR, "nba_draft_picks_rag.csv"),
    "team_stats": os.path.join(DATA_DIR, "total_team_stats.csv")
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
