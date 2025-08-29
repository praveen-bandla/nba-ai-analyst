import os
from dotenv import load_dotenv
from langchain.agents import initialize_agent, AgentType
from langchain.agents.agent import AgentExecutor
from langchain_openai import ChatOpenAI
from src.tools.tool_registry import RETRIEVER_TOOLS
from config.settings import RETRIEVAL_AGENT

load_dotenv()

class RetrievalAgent:
    def __init__(self):
        self.llm = ChatOpenAI(
            temperature=0,
            model=RETRIEVAL_AGENT,
            api_key=os.getenv("OPEN_AI_KEY")
        )
        self.tools = RETRIEVER_TOOLS
        self.agent: AgentExecutor = initialize_agent(
            tools=self.tools,
            llm=self.llm,
            agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
            verbose=True,
            handle_parsing_errors=True,
        )
        self.markdown_instruction = (
            "You are an expert NBA analyst. "
            "Always format your answers as Markdown. "
            "Use headings, bullet points, tables, and code blocks where appropriate. "
            "Add spaces and line breaks for readability. "
            "Never return plain text or unformatted output."
        )

    def invoke(self, query: str, history: list = None, **kwargs) -> str:
        """
        Run the agent and return a single string response.
        """
        if history is None:
            history = []
        full_query = self.markdown_instruction + "\n\n" + query
        return self.agent.run({"input": full_query, "chat_history": history})

    def stream(self, query: str, history: list = None, **kwargs):
        """
        Generator for streaming responses (token by token).
        If your LLM/tool supports streaming, implement here.
        Otherwise, yield the full response once.
        """
        # If your LLM supports streaming, use it here.
        # Otherwise, just yield the full response.
        response = self.invoke(query, history, **kwargs)
        for token in response.split(" "):
            yield token + " "

# Optional: CLI for quick testing
if __name__ == "__main__":
    agent = RetrievalAgent()
    chat_history = []
    while True:
        user_query = input("\n🧠 Ask your NBA question: ")
        if user_query.lower() in ["exit", "quit"]:
            break
        result = agent.invoke(user_query, chat_history)
        print("\n💬 Answer:", result)