from langchain_core.tools import Tool
from src.tools.base.base_retriever_tool import BaseRetrieverTool

tool = BaseRetrieverTool(
    dataset_key="team_stats",
    description="Answers questions about NBA team stats"
)

team_stats_tool = Tool(
    name="team_stats_tool",
    description=tool.description,
    func=tool.run
)
