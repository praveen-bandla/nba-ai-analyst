import os
import pandas as pd
from dotenv import load_dotenv
#from langchain.vectorstores import FAISS
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.docstore.document import Document
from config import settings  # ✅ central config

# Load environment variables
load_dotenv()

# === Config ===
DATA_PATH = settings.DATASETS["player_contracts"]
VECTORSTORE_DIR = settings.INDEX_PATHS["player_contracts"]
EMBEDDING_MODEL = settings.EMBEDDING_MODEL

# === Load Data ===
df = pd.read_csv(DATA_PATH).fillna("")

# === Construct Documents ===
documents = []


for _, row in df.iterrows():
    name = row["name"]
    team = row["team"]
    note = row["Note"]

    salaries = [row[f"Salary.{i}"] for i in range(6) if f"Salary.{i}" in row and row[f"Salary.{i}"] != ""]
    salary_str = ", ".join(salaries) if salaries else row["Salary"]

    content = f"""
    Player: {name}
    Team: {team}
    Contract: {salary_str}
    Notes: {note}
    """.strip()

    metadata = {
        "player": name,
        "team": team,
    }

    documents.append(Document(page_content=content, metadata=metadata))


# === Create Embeddings ===
embedding_model = OpenAIEmbeddings(model=EMBEDDING_MODEL, api_key=os.getenv("OPEN_AI_KEY"))
vectorstore = FAISS.from_documents(documents, embedding_model)

# === Save Vectorstore ===
os.makedirs(VECTORSTORE_DIR, exist_ok=True)
vectorstore.save_local(VECTORSTORE_DIR)

print(f"Player contract index built and saved to {VECTORSTORE_DIR}")
