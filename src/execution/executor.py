# src/execution/executor.py
from typing import Any, Dict, List
from langchain.tools import BaseTool
from src.tools.tool_registry import ALL_TOOLS

# Map legacy/simple retriever tool names (router output) -> structured aggregate tool names
ALIAS_MAP: Dict[str, str] = {
    "player_stats_tool": "player_stats_aggregate",
    "team_stats_tool": "team_stats_aggregate",
    "contracts_aggregate_tool": "contracts_aggregate",
    "team_capsheets_aggregate_tool": "team_capsheets_aggregate"
    # add more if needed
}

STRUCTURED_HINT_KEYS = {"metric", "metrics", "agg", "group_by", "filters", "players", "teams", "k"}

def build_tool_index(tools: List[BaseTool]) -> Dict[str, BaseTool]:
    return {t.name: t for t in tools}

def _is_structured(tool: BaseTool) -> bool:
    # LangChain StructuredTool / our aggregate Tools expose args_schema (or were created with a func expecting dict)
    return getattr(tool, "args_schema", None) is not None or tool.name.endswith("_aggregate")

def _resolve_tool_and_args(step: Dict[str, Any], name_to_tool: Dict[str, BaseTool]) -> tuple[BaseTool | None, Any, str]:
    raw_name = step.get("tool_name")
    args = step.get("args", {})
    # If router used retriever name but args look structured, redirect via alias
    resolved_name = ALIAS_MAP.get(raw_name, raw_name)

    tool = name_to_tool.get(resolved_name)
    if tool is None:
        return None, args, raw_name  # keep original for error message

    # If tool is unstructured (expects a single string) but args is a dict with structured keys:
    if isinstance(args, dict) and not _is_structured(tool):
        # Try to summarize dict to a string query OR redirect if alias exists
        # If we got here WITHOUT alias (no mapping) but args look structured, build a synthetic query string
        if any(k in args for k in STRUCTURED_HINT_KEYS):
            summary_parts = []
            for k, v in args.items():
                summary_parts.append(f"{k}={v}")
            args = "; ".join(summary_parts)  # simple fallback string
    return tool, args, resolved_name

def execute_ops(ops: Dict[str, Any], tools: List[BaseTool]) -> List[Dict[str, Any]]:
    name_to_tool = build_tool_index(tools)
    results: List[Dict[str, Any]] = []

    for step in ops.get("ops", []):
        if step.get("op") != "tool_call":
            results.append({"error": f"Unsupported op type '{step.get('op')}'"})
            continue

        tool, call_args, resolved_name = _resolve_tool_and_args(step, name_to_tool)

        if tool is None:
            results.append({"error": f"Unknown tool '{step.get('tool_name')}'"})
            continue

        try:
            # Structured aggregate tools accept dict directly
            print(call_args)
            out = tool.invoke(call_args)
        except Exception as e:
            out = {"error": f"{type(e).__name__}: {e}"}

        results.append({"tool": resolved_name, "output": out})

    return results

if __name__ == "__main__":
    """
    CLI to run: PlannerAgent -> RouterAgent -> execute_ops()

    Usage:
        PYTHONPATH=. python src/execution/executor.py
    """
    import json
    from typing import List
    from src.agents.planner_agent import PlannerAgent
    from src.agents.router_agent import RouterAgent

    def _load_tools() -> List[BaseTool]:
        return ALL_TOOLS

    tools = _load_tools()
    print("Loaded tools:", [t.name for t in tools])

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

            ops_dict = router.invoke(plan)
            print("\n=== Routed Ops ===")
            print(json.dumps(ops_dict, indent=2, ensure_ascii=False))

            exec_results = execute_ops(ops_dict, tools)
            print("\n=== Executor Results ===")
            print(json.dumps(exec_results, indent=2, ensure_ascii=False))

        except KeyboardInterrupt:
            print("\nInterrupted. Exiting.")
            break
        except Exception as e:
            print(f"\n[error] Unhandled exception: {type(e).__name__}: {e}")
