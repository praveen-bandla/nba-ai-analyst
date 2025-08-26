import pandas as pd
import os
from dotenv import load_dotenv
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import OpenAIEmbeddings
from langchain.docstore.document import Document
from config import settings  # centralized config

# === Load API Key ===
load_dotenv()

# === Config ===
DATA_PATH = settings.DATASETS["team_picks"]
VECTORSTORE_DIR = settings.INDEX_PATHS["team_picks"]
MODEL_NAME = settings.EMBEDDING_MODEL

# === Load Data ===
df = pd.read_csv(DATA_PATH)
df = df.fillna("")

# === Build Documents ===
documents = []

for _, row in df.iterrows():
    team = row["team"]
    year = row["year"]
    round_type = row["round"]
    details = row["details"]

    content = f"""
    Team: {team}
    Year: {year}
    Round: {round_type}
    Draft Pick Details: {details}
    """.strip()

    metadata = {
        "team": team,
        "year": year,
        "round": round_type,
    }

    documents.append(Document(page_content=content, metadata=metadata))

# === Create Vector Store ===
embedding_model = OpenAIEmbeddings(model=MODEL_NAME, api_key=os.getenv("OPEN_AI_KEY"))
vectorstore = FAISS.from_documents(documents, embedding_model)

# === Save to Disk ===
os.makedirs(VECTORSTORE_DIR, exist_ok=True)
vectorstore.save_local(VECTORSTORE_DIR)

print(f"Team picks index built and saved to {VECTORSTORE_DIR}")
