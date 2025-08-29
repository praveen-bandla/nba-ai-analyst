import time
from typing import List, Dict, Any
import streamlit as st
from src.agents.analysis_agent import run_pipeline  # analysis pipeline only

st.set_page_config(page_title="NBA AI Analyst (Analysis Only)", page_icon="🏀", layout="wide")
st.markdown("<h2>🏀 NBA AI Analyst</h2><p>Planner → Router → Executor → Synthesis (analysis path).</p>", unsafe_allow_html=True)
st.divider()

# Session state init
if "messages" not in st.session_state:
    st.session_state.messages: List[Dict[str, Any]] = [
        {"role": "assistant", "content": "Hi! Ask an NBA analysis question to start."}
    ]
if "pending_question" not in st.session_state:
    st.session_state.pending_question: str | None = None

# Sidebar with examples
with st.sidebar:
    st.subheader("Actions")
    if st.button("🧹 Clear Chat"):
        st.session_state.messages = [
            {"role": "assistant", "content": "Chat cleared. Ask another question!"}
        ]
        st.session_state.pending_question = None
        st.rerun()
    st.markdown("#### Quick Examples")
    if st.button("Buddy Hield vs Steph Curry 3PT%"):
        st.session_state.pending_question = "Who had a higher 3PT% in 2024-25, Buddy Hield or Steph Curry?"
        st.rerun()
    if st.button("Franz Wagner salary 2027-28"):
        st.session_state.pending_question = "What is Franz Wagner's salary 2027-28?"
        st.rerun()
    if st.button("Number of OKC picks in 2027"):
        st.session_state.pending_question = "How many picks does OKC have in 2027?"
        st.rerun()

    if st.button("Best offensive teams"):
        st.session_state.pending_question = "Which teams had the most number of points in 2024-25?"
        st.rerun()

    if st.button("Team with most capspace in 2027"):
        st.session_state.pending_question = "Which team has the most capspace in 2027?"
        st.rerun()


# Render existing history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"], avatar=("💡" if msg["role"] == "assistant" else "👤")):
        if msg["role"] == "assistant":
            st.markdown(msg["content"])
            if isinstance(msg, dict) and msg.get("debug"):
                with st.expander("Details", expanded=False):
                    st.markdown("**Plan**")
                    st.json(msg["debug"]["plan"])
                    st.markdown("**Routed Ops**")
                    st.json(msg["debug"]["ops"])
                    st.markdown("**Executor Results**")
                    st.json(msg["debug"]["exec_results"])
        else:
            st.write(msg["content"])

def handle_question(q: str):
    # Append user
    st.session_state.messages.append({"role": "user", "content": q})
    with st.chat_message("user", avatar="👤"):
        st.write(q)
    # Assistant
    with st.chat_message("assistant", avatar="💡"):
        placeholder = st.empty()
        try:
            placeholder.markdown("_Running analysis pipeline..._")
            plan, ops, exec_results, answer = run_pipeline(q)
            display = f"**Route:** `analyze`\n\n{answer}"
            acc = ""
            for line in display.splitlines(keepends=True):
                acc += line
                placeholder.markdown(acc + "▌")
                time.sleep(0.015)
            placeholder.markdown(acc)
            st.session_state.messages.append({
                "role": "assistant",
                "content": acc,
                "debug": {
                    "plan": plan,
                    "ops": ops,
                    "exec_results": exec_results
                }
            })
        except Exception as e:
            err = f"**Error:** {e}"
            placeholder.markdown(err)
            st.session_state.messages.append({"role": "assistant", "content": err})

# Process pending example (auto-run)
if st.session_state.pending_question:
    pq = st.session_state.pending_question
    st.session_state.pending_question = None
    handle_question(pq)

# Chat input (manual entry)
user_query = st.chat_input("Ask your NBA analysis question…")
if user_query:
    # User turn
    st.session_state.messages.append({"role": "user", "content": user_query})
    with st.chat_message("user", avatar="👤"):
        st.write(user_query)

    # Assistant turn (analysis pipeline)
    with st.chat_message("assistant", avatar="💡"):
        placeholder = st.empty()
        try:
            placeholder.markdown("_Running analysis pipeline..._")
            plan, ops, exec_results, answer = run_pipeline(user_query)
            display = f"**Route:** `analyze`\n\n{answer}"
            acc = ""
            for line in display.splitlines(keepends=True):
                acc += line
                placeholder.markdown(acc + "▌")
                time.sleep(0.015)
            placeholder.markdown(acc)
            st.session_state.messages.append({
                "role": "assistant",
                "content": acc,
                "debug": {
                    "plan": plan,
                    "ops": ops,
                    "exec_results": exec_results
                }
            })
        except Exception as e:
            err = f"**Error:** {e}"
            placeholder.markdown(err)
            st.session_state.messages.append({"role": "assistant", "content": err})