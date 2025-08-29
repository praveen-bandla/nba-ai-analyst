# src/agents/orchestrator_agent.py
from __future__ import annotations
import os
from typing import Dict, Any, Literal
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI

load_dotenv()

# Environment / model
MODEL_NAME = os.getenv("ORCHESTRATOR_AGENT", "gpt-4o-mini")
OPENAI_KEY = os.getenv("OPEN_AI_KEY")

LLM_SYSTEM = (
    "You are an intent router for NBA data.\n"
    "Return 'retrieve' for direct factual lookups (single player/team fact, single contract, one row).\n"
    "Return 'analyze' for aggregation, ranking, comparisons, multi-row outputs, statistics, or multi-step reasoning.\n"
    "Always choose exactly one.\n"
)

class OrchestratorDecision(BaseModel):
    route: Literal["retrieve", "analyze"] = Field(..., description="Selected high-level path.")
    reason: str
    confidence: float = Field(..., ge=0.0, le=1.0)
    used_llm: bool

class OrchestratorAgent:
    def __init__(self):
        self.llm = ChatOpenAI(
            model=MODEL_NAME, 
            temperature=0,
            api_key=OPENAI_KEY
            )

    def invoke(self, question: str) -> Dict[str, Any]:
        if not self.llm:
            return OrchestratorDecision(
                route="retrieve",
                reason="LLM disabled (no API key)",
                confidence=0.5,
                used_llm=False
            ).dict()

        user_prompt = (
            f"{LLM_SYSTEM}\n"
            "Respond with ONLY one word: retrieve OR analyze.\n"
            f"Question: {question}"
        )
        resp = self.llm.invoke(
            [
                {"role": "system", "content": LLM_SYSTEM},
                {"role": "user", "content": user_prompt},
            ]
        )
        text = (getattr(resp, "content", "") or "").strip().lower()
        if "analyze" in text:
            route = "analyze"
        elif "retrieve" in text:
            route = "retrieve"
        else:
            # Fallback: default to analyze (safer for multi-row questions)
            route = "analyze"
            text = f"Unrecognized response '{text}' -> default analyze"
        return OrchestratorDecision(
            route=route,
            reason=f"LLM response: {text}",
            confidence=0.65,
            used_llm=True
        ).dict()

    def stream(self, question: str):
        yield self.invoke(question)

# Manual test
if __name__ == "__main__":
    agent = OrchestratorAgent()
    tests = [
        "Who is LeBron James?",
        "What is the contract of Jalen Brunson?",
        "Top 5 teams by assists",
        "Compare average points and rebounds for Celtics and Knicks",
        "Which team has the highest payroll?",
        "List Nikola Jokic's contract details",
    ]
    for q in tests:
        print(f"\nQ: {q}")
        print(agent.invoke(q))
