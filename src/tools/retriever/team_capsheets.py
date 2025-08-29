from langchain_core.tools import Tool
from src.tools.base.base_retriever_tool import BaseRetrieverTool

tool = BaseRetrieverTool(
    dataset_key="team_capsheets",
    description="Answers questions about NBA team salary cap sheets"
)

team_capsheets_tool = Tool(
    name="team_capsheets_tool",
    description=tool.description,
    func=tool.run
)
