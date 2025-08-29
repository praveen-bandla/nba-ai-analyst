from langchain_core.tools import Tool
from src.tools.base.base_retriever_tool import BaseRetrieverTool

tool = BaseRetrieverTool(
    dataset_key="player_contracts",
    description="Answers questions about NBA player contracts"
)

player_contracts_tool = Tool(
    name="player_contracts_tool",
    description=tool.description,
    func=tool.run
)
