# src/agents/synthesis_agent.py
from __future__ import annotations
import os, json, traceback
from typing import Any, Dict, List, Optional, Iterable

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.runnables import Runnable

load_dotenv()

# Model name via env var (fallback)
MODEL_NAME = os.getenv("SYNTHESIS_AGENT_MODEL", "gpt-4o-mini")


class OutputSynthesisAgent:
    """
    Turns executor tool outputs + (optional) plan into a concise Markdown answer.
    Public methods: invoke(question, results, plan), stream(...)
    """

    def __init__(self, model: Optional[str] = None, temperature: float = 0.0):
        self.llm: Runnable = ChatOpenAI(
            model=model or MODEL_NAME,
            temperature=temperature,
            api_key=os.getenv("OPEN_AI_KEY"),
        )
        self.system_instruction = (
            "You are an NBA analytics synthesis agent.\n"
            "ONLY use provided tool outputs. Never invent players, teams, or numbers.\n"
            "Output MUST be Markdown.\n"
            "Guidelines:\n"
            "- Explain the contents of the tool with using your own domain knowledge of the nba\n"
            "- Dont introduce any new data, stick to the data provided\n"
            "- Some queries will require you to analyze the data and provide an answer. For instance: if the query is who earns more in 2026, banchero or wagner, the data may contain relevant salary information for both players. Analyze the data and provide a concise answer based on the evidence to the user query\n"
            "- Always offer the user a follow-up question to expand on the topic or ask relevant questions. For example, you could ask about other players' salaries or performance metrics.\n"
            "- Do not restate raw JSON; summarize.\n"
            "- You dont need to always return the games played stat if shown. If the user asks for something like points, rebounds or something, compute the relevant per game stats as well. Otherwise, if asked about accuracy or something, dont bother returning the games played."
            "- Always look through the notes and if the result columns are empty, print what the output of the notes are based on the relevance to the question."
        )
        self.plan_hint = (
            "Plan is intent guidance only. If plan conflicts with tool outputs, trust tool outputs.\n"
        )

    # ---------- Public API ----------
    def invoke(
        self,
        question: str,
        results: List[Dict[str, Any]],
        plan: Optional[Dict[str, Any]] = None,
        **_: Any,
    ) -> str:
        """
        Returns Markdown answer. Falls back to deterministic rendering on failure.
        """
        try:
            prompt = self._build_user_message(question, results, plan)
            resp = self.llm.invoke(
                [
                    {"role": "system", "content": self.system_instruction},
                    {"role": "user", "content": prompt},
                ]
            )
            text = getattr(resp, "content", "") or str(resp)
            if self._looks_markdown(text):
                return text.strip()
        except Exception:
            # Log (optional); in production you might route to logging infra
            traceback.print_exc()
        return self._fallback_markdown(question, results, plan)

    def stream(
        self,
        question: str,
        results: List[Dict[str, Any]],
        plan: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Iterable[str]:
        """Simple line streaming of final answer."""
        final = self.invoke(question, results, plan, **kwargs)
        for line in final.splitlines(True):
            yield line

    # ---------- Internal helpers ----------
    def _build_user_message(
        self, question: str, results: List[Dict[str, Any]], plan: Optional[Dict[str, Any]]
    ) -> str:
        plan_json = json.dumps(plan or {}, ensure_ascii=False, indent=2)
        results_json = json.dumps(results or [], ensure_ascii=False, indent=2)

        template_hint = (
            "Return ONLY Markdown.\n"
            "Suggested structure:\n"
            "## Answer\n"
            "One concise sentence.\n\n"
            "## Key Numbers\n"
            "- Metric: value (context)\n\n"
            "## Details\n"
            "- Method / filters / caveats\n\n"
            "## Data\n"
            "Markdown table (<=12 rows) if tabular.\n"
        )

        return (
            f"User Question:\n{question}\n\n"
            f"Plan (guidance, not authoritative):\n{self.plan_hint}{plan_json}\n\n"
            f"Tool Outputs (authoritative JSON):\n{results_json}\n\n"
            # f"{template_hint}"
        )

    def _looks_markdown(self, text: str) -> bool:
        t = (text or "").lstrip()
        return t

    def _fallback_markdown(
        self, question: str, results: List[Dict[str, Any]], plan: Optional[Dict[str, Any]]
    ) -> str:
        lines: List[str] = []
        lines.append("## Answer (Fallback)")
        lines.append("_LLM synthesis unavailable; showing raw structured output._\n")
        lines.append(f"**Question:** {question}\n")

        if plan:
            tf = (plan or {}).get("timeframe", {}) or {}
            lines.append("**Plan Summary:**")
            lines.append(
                f"- Dataset: `{plan.get('dataset')}`  Season: `{tf.get('season')}`  Goal: {plan.get('goal') or ''}".strip()
            )
            if plan.get("metric_hint"):
                lines.append(f"- Metric hint: `{plan['metric_hint']}`")
            lines.append("")

        if not results:
            lines.append("> No tool outputs returned.")
            return "\n".join(lines)

        for i, step in enumerate(results, 1):
            tool = step.get("tool") or "unknown_tool"
            out = step.get("output")
            lines.append(f"### Step {i}: `{tool}`")
            lines.extend(self._render_output(out))
            lines.append("")
        return "\n".join(lines).rstrip()

    def _render_output(self, out: Any) -> List[str]:
        if out is None:
            return ["_Empty._"]
        if isinstance(out, str):
            # Heuristic: already markdown table
            if out.strip().startswith("|"):
                return [out.strip()]
            return [out.strip()]
        if isinstance(out, dict):
            return [f"- **{k}:** {self._fmt(v)}" for k, v in list(out.items())[:24]]
        if isinstance(out, list):
            if not out:
                return ["_No rows._"]
            first = out[0]
            if isinstance(first, dict):
                headers = list(first.keys())[:10]
                rows_md = [
                    "| " + " | ".join(headers) + " |",
                    "| " + " | ".join(["---"] * len(headers)) + " |",
                ]
                for row in out[:12]:
                    rows_md.append(
                        "| " + " | ".join(self._fmt(row.get(h)) for h in headers) + " |"
                    )
                if len(out) > 12:
                    rows_md.append(f"_… {len(out)-12} more rows_")
                return rows_md
            # list of scalars
            return [f"- {self._fmt(v)}" for v in out[:25]] + (
                [f"_… {len(out)-25} more items_"] if len(out) > 25 else []
            )
        # Fallback
        return [str(out)]

    def _fmt(self, v: Any) -> str:
        if v is None:
            return "—"
        if isinstance(v, float):
            return f"{v:,.4g}"
        if isinstance(v, int):
            return f"{v:,}"
        return str(v)


# ---------- Simple CLI / test harness ----------
if __name__ == "__main__":
    agent = OutputSynthesisAgent()

    sample_question = "Which team had the highest 3PT% in 2024-25?"
    sample_plan = {
        "goal": "Find top team by 3PT%",
        "dataset": "team_stats",
        "timeframe": {"season": "2024-25"},
        "metric_hint": "three_pct",
    }
    sample_results = [
        {
            "tool": "team_stats_aggregate",
            "output": "| team | value |\n| --- | --- |\n| Denver Nuggets* | 0.376 |\n| Cleveland Cavaliers* | 0.383 |",
        }
    ]

    print("=== Synthesized Answer ===\n")
    print(agent.invoke(sample_question, sample_results, sample_plan))