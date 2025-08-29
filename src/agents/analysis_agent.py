from __future__ import annotations
import json
import traceback

from src.agents.planner_agent import PlannerAgent
from src.agents.router_agent import RouterAgent
from src.agents.synthesis_agent import OutputSynthesisAgent
from src.execution.executor import execute_ops
from src.tools.tool_registry import ALL_TOOLS


def run_pipeline(question: str) -> str:
    planner = PlannerAgent()
    router = RouterAgent()
    synth = OutputSynthesisAgent()

    # 1. Plan
    plan = planner.invoke(question)

    # 2. Route
    ops = router.invoke(plan)

    # 3. Execute tools
    exec_results = execute_ops(ops, ALL_TOOLS)

    # 4. Synthesize final answer
    answer = synth.invoke(question, exec_results, plan)
    return plan, ops, exec_results, answer


def interactive():
    print("NBA AI Analyst (planner -> router -> executor -> synthesis)")
    while True:
        try:
            q = input("\n🧭 Enter your NBA question (or 'quit'): ").strip()
            if q.lower() in {"quit", "exit"}:
                break

            plan, ops, exec_results, answer = run_pipeline(q)

            print("\n=== Planner Output ===")
            print(json.dumps(plan, indent=2, ensure_ascii=False))

            print("\n=== Routed Ops ===")
            print(json.dumps(ops, indent=2, ensure_ascii=False))

            print("\n=== Executor Results ===")
            print(json.dumps(exec_results, indent=2, ensure_ascii=False))

            print("\n=== Synthesis ===")
            print(answer)

        except KeyboardInterrupt:
            print("\nInterrupted.")
            break
        except Exception as e:
            print(f"[error] {type(e).__name__}: {e}")
            traceback.print_exc()


if __name__ == "__main__":
    interactive()