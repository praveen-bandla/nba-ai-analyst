from langchain_core.tools import Tool
from src.tools.base.base_retriever_tool import BaseRetrieverTool

tool = BaseRetrieverTool(
    dataset_key="team_picks",
    description="Answers questions about NBA team picks",
    num_results=20
)

team_picks_tool = Tool(
    name="team_picks_tool",
    description=tool.description,
    func=tool.run
)
