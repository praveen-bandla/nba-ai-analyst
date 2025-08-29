from langchain_core.tools import Tool
from src.tools.base.base_retriever_tool import BaseRetrieverTool

tool = BaseRetrieverTool(
    dataset_key="player_stats",
    description="Answers questions about NBA player stats"
)

player_stats_tool = Tool(
    name="player_stats_tool",
    description=tool.description,
    func=tool.run
)
