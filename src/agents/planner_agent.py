# src/agents/planner_agent.py
from __future__ import annotations
import os, re, json
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.pydantic_v1 import BaseModel, Field
from src.capabilities.manifest import MANIFEST
from config.settings import PLANNER_AGENT

load_dotenv()


# ---------- Pydantic schema for structured output ----------
class Entities(BaseModel):
    players: List[str] = Field(default_factory=list)
    teams: List[str] = Field(default_factory=list)

class Timeframe(BaseModel):
    season: Optional[str] = None

class Plan(BaseModel):
    goal: Optional[str] = None
    dataset: Optional[str] = None
    timeframe: Timeframe = Field(default_factory=Timeframe)
    entities: Entities = Field(default_factory=Entities)
    metric_hint: Optional[str] = None
    notes: List[str] = Field(default_factory=list)


# ---------- tiny helpers ----------
def _canon(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())

def _resolve_players(names: List[str], manifest: Dict[str, Any]) -> List[str]:
    if not names:
        return []
    aliases: Dict[str, List[str]] = manifest.get("player_aliases", {})
    rev: Dict[str, str] = {}
    for canon, alts in aliases.items():
        rev[_canon(canon)] = canon
        for a in alts:
            rev[_canon(a)] = canon
    out, seen = [], set()
    for n in names:
        k = _canon(n)
        canon = rev.get(k, n)
        ck = _canon(canon)
        if ck not in seen:
            out.append(canon)
            seen.add(ck)
    return out

def _resolve_season_from_text(text: str, manifest: Dict[str, Any]) -> Optional[str]:
    t = text.lower()
    for k, v in manifest["seasons"]["phrase_map"].items():
        if k in t:
            return v
    m = re.search(r"\b(20\d{2})\b", t)
    if m:
        y = int(m.group(1))
        return f"{y}-{str((y+1)%100).zfill(2)}"
    return None


class PlannerAgent:
    """
    Minimal planner aligned to your RetrievalAgent structure:
    - LLM with structured output (Pydantic) -> avoids JSON parsing.
    - No tools, no LangGraph; just returns a semantic plan dict.
    - Small manifest context + light deterministic normalization.
    """

    def __init__(self):
        self.llm = ChatOpenAI(
            temperature=0,
            model=PLANNER_AGENT,
            api_key=os.getenv("OPEN_AI_KEY"),
        )
        # keep the prompt tiny: datasets, metrics, aliases
        self.datasets = list(MANIFEST.get("tables", {}).keys())
        self.metric_keys = list(MANIFEST.get("glossary", {}).get("metrics", {}).keys())
        self.team_aliases = MANIFEST.get("team_aliases", {})
        self.defaults = MANIFEST["seasons"]["defaults"]
        self.season_map = MANIFEST["seasons"]["phrase_map"]
        self.last_stats_season = self.season_map['last year']
        self.player_aliases = MANIFEST.get("player_aliases", {})

        self.system_instruction = (
            "You are an NBA analytics *planner*.\n"
            "Return ONLY a JSON object matching this schema: "
            "{goal, dataset, timeframe:{season}, entities:{players[], teams[]}, metric_hint, notes[]}.\n"
            f"Make sure the player names are correctly capitalized and listed as per the database. for example: Stephen Curry, LeBron James, Utilize the player aliases {self.player_aliases} if relevant to interpret the query. Make sure to list the correct player names"
            f"Allowed datasets: {self.datasets}.\n"
            f"Metric names (hint list): {self.metric_keys}.\n"
            f"Team aliases (keys=canonical, values=aliases): {self.team_aliases}.\n"
            "Prefer canonical full names for players and teams.\n"
            "Do NOT write SQL or execution steps."
            f"The player_stats, team_stats are only provided for the {self.last_stats_season} season. If no year is specified, assume that the query asks about {self.last_stats_season}. Refer to this map for further guidance: {self.season_map}. If a specific year is specified, use that"
        )

    # ---------- public API (mirrors your RetrievalAgent) ----------
    def invoke(self, query: str, history: list = None, **kwargs) -> Dict[str, Any]:
        """
        Produce a semantic plan for `query`.
        Returns a Python dict with keys:
        goal, dataset, timeframe.season, entities.players, entities.teams, metric_hint, notes.
        """
        structured = self.llm.with_structured_output(Plan, method="function_calling")
        draft: Plan = structured.invoke(
            [
                {"role": "system", "content": self.system_instruction},
                {"role": "user", "content": f"Question: {query}"},
            ]
        )

        plan = draft.dict()

        # Normalize nicknames -> canonical player names
        plan["entities"]["players"] = _resolve_players(plan["entities"].get("players", []), MANIFEST)

        # Season inference/defaults
        season = plan.get("timeframe", {}).get("season") or _resolve_season_from_text(query, MANIFEST)
        print(f'The selected season is: {season}')
        if not season:
            ds = plan.get("dataset")
            if ds == "player_contracts":
                season = self.defaults["salary"]
            elif ds in ("player_stats", "team_stats"):
                season = self.defaults["stats"]
        plan["timeframe"]["season"] = season

        # Metric hint normalization (lowercase match against manifest keys)
        metric = (plan.get("metric_hint") or "").lower()
        gloss = MANIFEST.get("glossary", {}).get("metrics", {})
        if metric in gloss:
            plan["metric_hint"] = gloss[metric] or metric
        elif not metric:
            plan["metric_hint"] = None

        plan.setdefault("notes", [])
        return plan

    def stream(self, query: str, history: list = None, **kwargs):
        """
        Yield pretty-printed JSON lines (simple streaming for your UI).
        """
        result = self.invoke(query, history=history, **kwargs)
        pretty = json.dumps(result, indent=2)
        for line in pretty.splitlines(True):
            yield line


# Optional: CLI for quick testing
if __name__ == "__main__":
    agent = PlannerAgent()
    while True:
        q = input("\n🧭 Planning for: ")
        if q.lower() in {"exit", "quit"}:
            break
        plan = agent.invoke(q)
        print(json.dumps(plan, indent=2))
