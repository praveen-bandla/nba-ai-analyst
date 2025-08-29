# src/tools/tool_registry.py

# Retriever tools (direct data lookups)
from src.tools.retriever.player_contracts import player_contracts_tool
from src.tools.retriever.player_stats import player_stats_tool
from src.tools.retriever.team_capsheets import team_capsheets_tool
from src.tools.retriever.team_picks import team_picks_tool
from src.tools.retriever.team_stats import team_stats_tool

# Compute / aggregation tools (factory-produced, already instantiated in their modules)
from src.tools.compute.player_contracts import contracts_aggregate_tool
from src.tools.compute.player_stats import player_stats_aggregate_tool
from src.tools.compute.team_capsheets import team_capsheets_aggregate_tool
from src.tools.compute.team_picks import team_picks_aggregate_tool
from src.tools.compute.team_stats import team_stats_aggregate_tool

# Master registry (order can matter if an agent picks first match)
ALL_TOOLS = [
    # Retriever
    player_contracts_tool,
    player_stats_tool,
    team_capsheets_tool,
    team_picks_tool,
    team_stats_tool,

    # Aggregation / compute
    contracts_aggregate_tool,
    player_stats_aggregate_tool,
    team_capsheets_aggregate_tool,
    team_picks_aggregate_tool,
    team_stats_aggregate_tool
]

RETRIEVER_TOOLS = [
    player_contracts_tool,
    player_stats_tool,
    team_capsheets_tool,
    team_picks_tool,
    team_stats_tool
]