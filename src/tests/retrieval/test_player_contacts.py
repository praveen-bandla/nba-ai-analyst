import os
from dotenv import load_dotenv
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from config import settings  # centralized config

# Load your API key
load_dotenv()
embedding_model = OpenAIEmbeddings(model=settings.EMBEDDING_MODEL, api_key=os.getenv("OPEN_AI_KEY"))

# Load FAISS index
vectorstore = FAISS.load_local(
    settings.INDEX_PATHS["player_contracts"], 
    embedding_model,
    allow_dangerous_deserialization=True
)

# Set up retriever
retriever = vectorstore.as_retriever(search_type="similarity", search_kwargs={"k": 1})

# Sample user query
query = "What is siakam's contract?"

# Run retrieval
docs = retriever.get_relevant_documents(query)

# Display results
for i, doc in enumerate(docs):
    print(f"\n--- Document {i+1} ---")
    print(doc.page_content)
