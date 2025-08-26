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
DATA_PATH = settings.DATASETS["team_stats"]
VECTORSTORE_DIR = settings.INDEX_PATHS["team_stats"]
MODEL_NAME = settings.EMBEDDING_MODEL

# === Load Data ===
df = pd.read_csv(DATA_PATH)
df = df.fillna("")

# === Build Documents ===
documents = []

for _, row in df.iterrows():
    team = row["Team"].replace("*", "")  # Remove asterisk if present
    games = row["G"]
    fg_pct = row["FG%"]
    tp_pct = row["3P%"]
    ft_pct = row["FT%"]
    trb = row["TRB"]
    ast = row["AST"]
    tov = row["TOV"]
    pts = row["PTS"]

    content = f"""
    Team: {team}
    Games Played: {games}
    Field Goal %: {fg_pct}
    Three Point %: {tp_pct}
    Free Throw %: {ft_pct}
    Total Rebounds: {trb}
    Assists: {ast}
    Turnovers: {tov}
    Total Points: {pts}
    """.strip()

    metadata = {
        "team": team,
    }

    documents.append(Document(page_content=content, metadata=metadata))

# === Create Vector Store ===
embedding_model = OpenAIEmbeddings(model=MODEL_NAME, api_key=os.getenv("OPEN_AI_KEY"))
vectorstore = FAISS.from_documents(documents, embedding_model)

# === Save to Disk ===
os.makedirs(VECTORSTORE_DIR, exist_ok=True)
vectorstore.save_local(VECTORSTORE_DIR)

print(f"Team stats index built and saved to {VECTORSTORE_DIR}")
