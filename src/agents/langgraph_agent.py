from __future__ import annotations
from typing import TypedDict, Dict, Any, List
from langgraph.graph import StateGraph, END

from src.agents.orchestrator_agent import OrchestratorAgent
from src.agents.retrieval_agent import RetrievalAgent
from src.agents.planner_agent import PlannerAgent
from src.agents.router_agent import RouterAgent
from src.agents.synthesis_agent import OutputSynthesisAgent
from src.execution.executor import execute_ops
from src.tools.tool_registry import ALL_TOOLS
from src.graphs.main_graph import build_main_graph


class PipelineState(TypedDict, total=False):
    question: str
    route: str                 # 'retrieve' | 'analyze'
    plan: Dict[str, Any]
    ops: Dict[str, Any]
    results: List[Dict[str, Any]]
    answer_markdown: str


# Instantiate stateless / lightweight agents once
_orch = OrchestratorAgent()
_planner = PlannerAgent()
_router = RouterAgent()
_synth = OutputSynthesisAgent()


def _n_orchestrator(state: PipelineState) -> PipelineState:
    dec = _orch.invoke(state["question"])
    return {**state, "route": dec["route"]}


def _n_retrieve(state: PipelineState) -> PipelineState:
    # RetrievalAgent is lightweight; instantiate per call (or could cache)
    retriever = RetrievalAgent()
    raw = retriever.invoke(state["question"])
    # Normalize through synthesis for consistent formatting
    answer = _synth.invoke(state["question"], [{"tool": "retriever", "output": raw}], None)
    return {**state, "answer_markdown": answer}


def _n_planner(state: PipelineState) -> PipelineState:
    plan = _planner.invoke(state["question"])
    return {**state, "plan": plan}


def _n_router(state: PipelineState) -> PipelineState:
    ops = _router.invoke(state["plan"])
    return {**state, "ops": ops}


def _n_executor(state: PipelineState) -> PipelineState:
    results = execute_ops(state["ops"], ALL_TOOLS)
    return {**state, "results": results}


def _n_synth_analyze(state: PipelineState) -> PipelineState:
    answer = _synth.invoke(state["question"], state.get("results", []), state.get("plan"))
    return {**state, "answer_markdown": answer}


def _branch(state: PipelineState) -> str:
    return "retrieve" if state.get("route") == "retrieve" else "analyze"


def build_langgraph():
    g = StateGraph(PipelineState)
    g.set_entry_point("orchestrator")

    g.add_node("orchestrator", _n_orchestrator)
    g.add_node("retriever", _n_retrieve)
    g.add_node("planner", _n_planner)
    g.add_node("router", _n_router)
    g.add_node("executor", _n_executor)
    g.add_node("synth_analyze", _n_synth_analyze)

    g.add_conditional_edges(
        "orchestrator",
        _branch,
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


class LangGraphAgent:
    """
    Simple wrapper exposing .invoke(question) and .stream(question)
    """
    def __init__(self):
        self.app = build_main_graph()

    def invoke(self, question: str) -> Dict[str, Any]:
        return self.app.invoke({"question": question})

    def stream(self, question: str):
        # For now just yield final answer (could expand with intermediate callbacks)
        state = self.invoke(question)
        yield state.get("answer_markdown", "")


if __name__ == "__main__":
    agent = LangGraphAgent()
    print("🏀 LangGraph Agent CLI (type 'quit' to exit)")
    while True:
        q = input("\nQuestion: ").strip()
        if q.lower() in {"quit", "exit"}:
            break
        state = agent.invoke(q)
        print(f"\nRoute: {state.get('route')}")
        if state.get("error"):
            print(f"\nError: {state['error']}")
        print("\nAnswer:\n")
        print(state.get("answer_markdown") or "No answer.")
        print("\n[debug keys]", list(state.keys()))