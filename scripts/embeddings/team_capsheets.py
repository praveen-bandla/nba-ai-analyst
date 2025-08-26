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
DATA_PATH = settings.DATASETS["team_capsheets"]
VECTORSTORE_DIR = settings.INDEX_PATHS["team_capsheets"]
MODEL_NAME = settings.EMBEDDING_MODEL

# === Load Data ===
df = pd.read_csv(DATA_PATH, skiprows=1)  # skip the extra header row (i.e., `,,Salary,Salary,...`)
df = df.fillna("")  # fill blanks

# === Build Documents ===
documents = []

for _, row in df.iterrows():
    team = row["Team"]

    # Extract salaries by year
    salary_by_year = {
        "2025-26": row.get("2025-26", ""),
        "2026-27": row.get("2026-27", ""),
        "2027-28": row.get("2027-28", ""),
        "2028-29": row.get("2028-29", ""),
        "2029-30": row.get("2029-30", ""),
        "2030-31": row.get("2030-31", ""),
    }

    # Format salary info
    salary_str = "\n".join([f"{year}: {salary}" for year, salary in salary_by_year.items() if salary])

    content = f"""
    Team: {team}
    Future Salary Commitments:
    {salary_str}
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

print(f"Team cap sheet index built and saved to {VECTORSTORE_DIR}")
