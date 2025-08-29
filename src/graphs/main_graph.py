# src/graphs/main_graph.py
from __future__ import annotations
from typing import TypedDict, Dict, Any, List
from langgraph.graph import StateGraph, END

from src.agents.orchestrator_agent import OrchestratorAgent
from src.agents.planner_agent import PlannerAgent
from src.agents.router_agent import RouterAgent
from src.agents.synthesis_agent import OutputSynthesisAgent
from src.agents.retrieval_agent import RetrievalAgent   # FIX: correct import (was agents.retrieval_agent)
from src.execution.executor import execute_ops
from src.tools.tool_registry import ALL_TOOLS

# ---------- State ----------
class PipelineState(TypedDict, total=False):
    question: str
    route: str                # retrieve | analyze
    plan: Dict[str, Any]
    ops: Dict[str, Any]
    results: List[Dict[str, Any]]
    answer_markdown: str
    error: str

# ---------- Agents ----------
orch = OrchestratorAgent()
planner = PlannerAgent()
router = RouterAgent()
synth = OutputSynthesisAgent()

# ---------- Nodes ----------
def n_orchestrator(state: PipelineState) -> PipelineState:
    dec = orch.invoke(state["question"])
    return {**state, "route": dec["route"]}

def n_planner(state: PipelineState) -> PipelineState:
    plan = planner.invoke(state["question"])
    return {**state, "plan": plan}

def n_router(state: PipelineState) -> PipelineState:
    ops = router.invoke(state["plan"])
    return {**state, "ops": ops}

def n_executor(state: PipelineState) -> PipelineState:
    try:
        results = execute_ops(state["ops"], ALL_TOOLS)
        return {**state, "results": results}
    except Exception as e:
        return {**state, "error": f"executor_error: {e}", "results": []}

def n_synth_analyze(state: PipelineState) -> PipelineState:
    answer = synth.invoke(
        state["question"],
        results=state.get("results", []),
        plan=state.get("plan"),
    )
    return {**state, "answer_markdown": answer}

def n_retriever(state: PipelineState) -> PipelineState:
    retriever = RetrievalAgent()
    try:
        raw = retriever.invoke(state["question"])
        answer = synth.invoke(
            state["question"],
            results=[{"tool": "retriever", "output": raw}],
            plan=None,
        )
        return {**state, "answer_markdown": answer}
    except Exception as e:
        return {**state, "error": f"retrieval_error: {e}", "answer_markdown": f"Retrieval failed: {e}"}

# ---------- Routing ----------
def branch(state: PipelineState) -> str:
    return "retrieve" if state.get("route") == "retrieve" else "analyze"

# ---------- Graph Builder ----------
def build_main_graph():
    g = StateGraph(PipelineState)
    g.set_entry_point("orchestrator")

    g.add_node("orchestrator", n_orchestrator)
    g.add_node("planner", n_planner)
    g.add_node("router", n_router)
    g.add_node("executor", n_executor)
    g.add_node("synth_analyze", n_synth_analyze)
    g.add_node("retriever", n_retriever)

    g.add_conditional_edges(
        "orchestrator",
        branch,
        {
            "retrieve": "retriever",
            "analyze": "planner",
        },
    )

    g.add_edge("planner", "router")
    g.add_edge("router", "executor")
    g.add_edge("executor", "synth_analyze")
    g.add_edge("retriever", END)
    g.add_edge("synth_analyze", END)

    return g.compile()

# ---------- CLI ----------
if __name__ == "__main__":
    graph = build_main_graph()
    print("🏀 NBA Analytics Graph CLI (orchestrator -> retrieve|analyze)")
    while True:
        try:
            q = input("\n🧭 Question (or 'quit'): ").strip()
            if q.lower() in {"quit", "exit"}:
                break
            state = graph.invoke({"question": q})
            print(f"\nRoute: {state.get('route')}")
            if state.get("error"):
                print(f"\nError: {state['error']}")
            print("\n=== Final Answer ===\n")
            print(state.get("answer_markdown") or "No answer.")
            # Debug keys
            print("\n[debug keys]", list(state.keys()))
        except KeyboardInterrupt:
            print("\nExiting.")
            break
        except Exception as e:
            print(f"\nFatal error: {e}")
