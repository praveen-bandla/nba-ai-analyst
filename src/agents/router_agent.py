# src/agents/router_agent.py
from __future__ import annotations
import os, json
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field, model_validator  # CHANGED: import model_validator
from config.settings import ROUTER_AGENT
from src.capabilities.manifest import MANIFEST  # optional context
from src.tools.tool_registry import ALL_TOOLS  # you already have this

load_dotenv()

# ---------- Output schema ----------
class ToolOp(BaseModel):
    op: str = Field(description='Operation type; use "tool_call" for now', default="tool_call")
    tool_name: str = Field(description="Name of the tool to call (must be in allowed_tools)")
    args: Dict[str, Any] = Field(default_factory=dict, description="Arguments JSON for the tool")

class RoutePlan(BaseModel):
    ops: List[ToolOp] = Field(default_factory=list)

    @model_validator(mode="after")
    def _non_empty_ops(self):
        if not self.ops:
            raise ValueError("ops must not be empty")
        return self

# ---------- Router Agent ----------
class RouterAgent:
    """
    Turns a planner Plan into an executable op list:
    - Chooses tool(s) from ALL_TOOLS based on dataset/intent
    - Crafts args from entities / timeframe / metric_hint
    - Returns RoutePlan.ops = [{op, tool_name, args}, ...]
    """

    def __init__(self):
        # The LLM that maps a Plan -> ops list
        self.llm = ChatOpenAI(
            temperature=0,
            model=ROUTER_AGENT,
            api_key=os.getenv("OPEN_AI_KEY"),
        )

        # Build a tiny catalog the model can "see"
        # Map dataset -> tool_name (adjust to your actual tool names)
        self.dataset_to_tool = {
            "player_stats": "player_stats_aggregate_tool",
            "team_stats": "team_stats_aggregate_tool",
            "player_contracts": "contracts_aggregate_tool",
            "team_picks": "team_picks_aggregate_tool",
            "team_capsheets": "team_capsheets_aggregate_tool",
        }

        # Derive allowed tool names from your registry to gate LLM output
        self.allowed_tools = sorted({t.name for t in ALL_TOOLS})  # assumes each tool has .name

        # Brief arg “shapes” to guide the LLM (now with aggregates)
        self.tool_arg_hints = {
            # Retrieval + optional aggregation on metrics
            "player_stats_aggregate_tool": {
                "season": "YYYY-YY",
                "players": ["..."],
                "teams": ["..."],
                "metrics": ["..."],            # e.g., ["points", "ast", "ts%"]
                "agg": "avg|sum|count|min|max|median|pXX",  # choose ONE; pXX = percentile like p90
                "group_by": ["team|player|position|none"],  # optional; omit if not needed
                "k": 20                                     # optional top-k
            },
            "team_stats_aggregate_tool": {
                "season": "YYYY-YY",
                "teams": ["..."],
                "metrics": ["..."],
                "agg": "avg|sum|count|min|max|median|pXX",
                "group_by": ["team|division|conference|none"],
                "k": 20
            },
            # Contracts/salary analytics; allow season as YYYY or YYYY-YY (your executor can normalize)
            "contracts_aggregate_tool": {
                "season": "YYYY or YYYY-YY",
                "players": ["..."],
                "teams": ["..."],
                "metric": "salary|cap_hit|guaranteed|aav",  # pick ONE metric
                "agg": "avg|sum|count|min|max|median|pXX",  # choose ONE aggregate
                "group_by": ["team|none"],                  # optional
            },
            "team_picks_aggregate_tool": {
                "teams": ["..."],
                "season": "YYYY-YY",
                "agg": "count|none",                        # usually counts
                "group_by": ["team|round|none"],
            },
            "team_capsheets_aggregate_tool": {
                "teams": ["..."],
                "season": "YYYY-YY",
                "metric": "cap_space",
                "agg": "avg|sum|min|max|none",
                "group_by": ["team|none"],
                "limit": 50
            },
        }

        self.player_aliases = MANIFEST.get("player_aliases", {})
        self.team_aliases = MANIFEST.get("team_aliases", {})

        # Lightweight router instruction (emphasize aggregates)
        self.system_instruction = (
            "You are an OP router for an NBA analytics system.\n"
            f"Allowed tools: {self.allowed_tools}.\n"
            f"Dataset→tool: {self.dataset_to_tool}.\n"
            f"Arg hints: {self.tool_arg_hints}.\n"
            "Return JSON: {ops:[{op:'tool_call', tool_name:'<allowed>', args:{...}}]}.\n"
            f"Match the player names to exactly what the correct capitalization would be in the database, for example: LeBron James, Giannis Antetokounmpo. Utilize the player aliases {self.player_aliases} if relevant to interpret the input. Make sure to list the correct player names."
            f"Match team names to the canonical names in the database, e.g. 'Los Angeles Lakers' not 'Lakers'. Utilize the team aliases {self.team_aliases} if relevant to interpret the input. Make sure to list the correct team names."
        )


    # ---------- public API ----------
    def invoke(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a Plan dict into {ops: [...]}.
        """
        # Choose structured output via function_calling to avoid pydantic v1 warnings
        structured = self.llm.with_structured_output(RoutePlan, method="function_calling")

        # Minimal normalization/safety: pick a suggested tool if dataset present
        suggested_tool = None
        dataset = (plan.get("dataset") or "").strip()
        if dataset in self.dataset_to_tool:
            suggested_tool = self.dataset_to_tool[dataset]

        messages = [
            {"role": "system", "content": self.system_instruction},
            {"role": "user", "content": json.dumps({"plan": plan, "suggested_tool": suggested_tool}, ensure_ascii=False)},
        ]
        route: RoutePlan = structured.invoke(messages)

        # Post-check: ensure tool_name is allowed
        for op in route.ops:
            if op.tool_name not in self.allowed_tools:
                # hard guardrail: replace with suggested or first allowed to avoid executor blowups
                op.tool_name = suggested_tool or self._fallback_tool()
        return route.dict()

    def stream(self, plan: Dict[str, Any]):
        result = self.invoke(plan)
        import json as _json
        pretty = _json.dumps(result, indent=2)
        for line in pretty.splitlines(True):
            yield line

    # ---------- helpers ----------
    def _fallback_tool(self) -> str:
        # Pick something safe/deterministic
        return "player_stats_aggregate_tool" if "player_stats_aggregate_tool" in self.allowed_tools else (self.allowed_tools[0] if self.allowed_tools else "unknown_tool")


# --- at the bottom of src/agents/router_agent.py ---
if __name__ == "__main__":
    import json
    from src.agents.planner_agent import PlannerAgent  # 1) import PlannerAgent first

    planner = PlannerAgent()
    router = RouterAgent()

    while True:
        try:
            user_q = input("\n🧭 Enter your NBA question (or 'quit'): ").strip()
            if user_q.lower() in {"quit", "exit"}:
                break

            plan = planner.invoke(user_q)
            print("\n=== Planner Output ===")
            print(json.dumps(plan, indent=2, ensure_ascii=False))

            ops = router.invoke(plan)
            print("\n=== Routed Ops ===")
            print(json.dumps(ops, indent=2, ensure_ascii=False))
        except KeyboardInterrupt:
            break
