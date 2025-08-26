import pandas as pd
import os
from dotenv import load_dotenv
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import OpenAIEmbeddings
from langchain.docstore.document import Document
from config import settings  # your centralized config

# === Load API Key ===
load_dotenv()

# === Config ===
DATA_PATH = settings.DATASETS["player_stats"]
VECTORSTORE_DIR = settings.INDEX_PATHS["player_stats"]
MODEL_NAME = settings.EMBEDDING_MODEL

# === Load Data ===
df = pd.read_csv(DATA_PATH)
df = df.fillna("")  # fill missing values

# === Build Documents ===
documents = []

for _, row in df.iterrows():
    name = row["Player"]
    team = row["Team"]
    age = row["Age"]
    games = row["G"]
    points = row["PTS"]
    assists = row["AST"]
    rebounds = row["TRB"]
    awards = row["Awards"]
    
    # Build the readable content
    content = f"""
    Player: {name}
    Team: {team}
    Age: {age}
    Games Played: {games}
    Points: {points}
    Assists: {assists}
    Rebounds: {rebounds}
    Awards: {awards}
    """.strip()

    metadata = {
        "player": name,
        "team": team,
    }

    documents.append(Document(page_content=content, metadata=metadata))

# === Create Vector Store ===
embedding_model = OpenAIEmbeddings(model=MODEL_NAME, api_key=os.getenv("OPEN_AI_KEY"))
vectorstore = FAISS.from_documents(documents, embedding_model)

# === Save to Disk ===
os.makedirs(VECTORSTORE_DIR, exist_ok=True)
vectorstore.save_local(VECTORSTORE_DIR)

print(f"Player stats index built and saved to {VECTORSTORE_DIR}")
