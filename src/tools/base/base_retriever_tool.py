# scripts/tools/base/base_retriever_tool.py
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from config import settings
import os
from dotenv import load_dotenv

load_dotenv()

class BaseRetrieverTool:
    def __init__(self, dataset_key: str, description: str, num_results: int = 1):
        self.dataset_key = dataset_key
        self.description = description
        self.embedding_model = OpenAIEmbeddings(
            model=settings.EMBEDDING_MODEL,
            api_key=os.getenv("OPEN_AI_KEY")
        )
        self.vectorstore = FAISS.load_local(
            settings.INDEX_PATHS[dataset_key],
            self.embedding_model,
            allow_dangerous_deserialization=True
        )
        self.retriever = self.vectorstore.as_retriever(search_type="similarity", search_kwargs={"k": num_results})

    def run(self, query: str) -> str:
        docs = self.retriever.get_relevant_documents(query)
        return "\n\n".join([doc.page_content for doc in docs])

