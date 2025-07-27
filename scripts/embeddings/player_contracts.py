import pandas as pd
import os
from langchain.vectorstores import FAISS
from langchain.embeddings import OpenAIEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.docstore.document import Document
from dotenv import load_dotenv

# Load environment variables (for OpenAI API key)
load_dotenv()

# === Config ===
DATA_PATH = "data/player_contracts_with_notes.csv"
VECTORSTORE_DIR = "vectorstores/player_contracts_index"

# === Load Data ===
df = pd.read_csv(DATA_PATH)

# Fill NaNs with empty strings (e.g., missing salary years)
df = df.fillna("")

# === Construct Documents ===
documents = []

for _, row in df.iterrows():
    name = row["name"]
    team = row["team"]
    note = row["Note"]

    # Combine salary years into a readable string
    salaries = [row[f"Salary.{i}"] for i in range(6) if f"Salary.{i}" in row and row[f"Salary.{i}"] != ""]
    salary_str = ", ".join(salaries) if salaries else row["Salary"]

    # Format text chunk
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

# === Optional: Split (if large documents, not needed here) ===
# splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
# documents = splitter.split_documents(documents)

# === Create Embeddings ===
embedding_model = OpenAIEmbeddings()
vectorstore = FAISS.from_documents(documents, embedding_model)

# === Save Vectorstore ===
os.makedirs(VECTORSTORE_DIR, exist_ok=True)
vectorstore.save_local(VECTORSTORE_DIR)

print(f"✅ Player contract index built and saved to {VECTORSTORE_DIR}")
