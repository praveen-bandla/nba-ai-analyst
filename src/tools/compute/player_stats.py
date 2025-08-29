# tools/compute/player_stats.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
import duckdb
from pydantic import BaseModel, Field, field_validator
from langchain.tools import StructuredTool

PARQUET_PATH = "data/parquet/player_stats.parquet"
DEFAULT_SEASON = "2024-25"

# --- DuckDB connection cache ---
_duck_con: duckdb.DuckDBPyConnection | None = None
def _con() -> duckdb.DuckDBPyConnection:
    global _duck_con
    if _duck_con is None:
        _duck_con = duckdb.connect(database=":memory:")
    return _duck_con

# --- Args schema for StructuredTool ---
class PlayerStatsAggregateArgs(BaseModel):
    season: Optional[str] = Field(default=None)
    metric: Optional[str] = Field(default=None)
    metrics: Optional[List[str]] = Field(default=None)
    agg: str = Field(default="avg")
    group_by: Any = Field(default="none", description="String or single-item list for grouping")
    players: Optional[List[str]] = None
    teams: Optional[List[str]] = None
    filters: Optional[Dict[str, Any]] = Field(default=None)
    k: Optional[int] = Field(default=None)

    @field_validator("group_by", mode="before")
    @classmethod
    def _coerce_group_by(cls, v):
        if isinstance(v, list):
            return v[0] if v else "none"
        return v

# --- Helpers (unchanged logic) ---
def _coalesce_metrics(metric: Optional[str], metrics: Optional[List[str]]) -> List[str]:
    if metrics:
        return list(dict.fromkeys(metrics))
    if metric:
        return [metric]
    # No metric(s) provided -> we will default to a synthetic count metric upstream
    return []

def _build_where(a: Dict[str, Any]) -> str:
    clauses: List[str] = []
    if a.get("season"):
        clauses.append("season = ?")
    if a.get("players"):
        clauses.append("player IN (" + ",".join(["?"] * len(a["players"])) + ")")
    if a.get("teams"):
        clauses.append("team IN (" + ",".join(["?"] * len(a["teams"])) + ")")
    filters = a.get("filters") or {}
    op_map = {"gte": ">=", "lte": "<=", "gt": ">", "lt": "<", "eq": "="}
    for k, v in filters.items():
        if "__" in k:
            col, suf = k.split("__", 1)
            if suf in op_map:
                clauses.append(f"{col} {op_map[suf]} ?")
        else:
            clauses.append(f"{k} = ?")
    return " AND ".join(clauses) if clauses else "TRUE"

def _build_params(a: Dict[str, Any]) -> List[Any]:
    params: List[Any] = []
    if a.get("season"):
        params.append(a["season"])
    if a.get("players"):
        params.extend(a["players"])
    if a.get("teams"):
        params.extend(a["teams"])
    filters = a.get("filters") or {}
    for k, v in filters.items():
        if v is None:
            continue
        if "__" in k:
            params.append(v)
        else:
            params.append(v)
    return params

def _agg_expr(col: str, agg: str) -> str:
    agg_l = agg.lower()
    if agg_l.startswith("p"):
        try:
            q = int(agg_l[1:]) / 100.0
            return f"quantile({col}, {q})"
        except Exception:
            pass
    if agg_l == "count":
        return "COUNT(*)"
    if agg_l == "median":
        return f"median({col})"
    return f"{agg_l.upper()}({col})"

def _build_sql(a: Dict[str, Any]) -> str:
    where_sql = _build_where(a)
    group_by = a["group_by"]
    agg = a["agg"]
    metrics = a["metrics"]

    # Special default COUNT path (synthetic metric name)
    if len(metrics) == 1 and metrics[0] == "row_count" and agg == "count":
        if group_by == "none":
            return (
                f"SELECT COUNT(*) AS row_count, SUM(g) AS games_played "
                f"FROM read_parquet('{PARQUET_PATH}') WHERE {where_sql}"
            )
        else:
            sql = (
                f"SELECT {group_by} AS {group_by}, COUNT(*) AS value, SUM(g) AS games_played "
                f"FROM read_parquet('{PARQUET_PATH}') WHERE {where_sql} GROUP BY {group_by}"
            )
            if a.get("k"):
                sql += f" ORDER BY value DESC LIMIT {int(a['k'])}"
            return sql

    # Multiple metrics + grouping -> UNION ALL
    if len(metrics) > 1 and group_by != "none":
        subs = [
            f"SELECT {group_by} AS group_key, {_agg_expr(m, agg)} AS value, "
            f"'{m}' AS metric, SUM(g) AS games_played "
            f"FROM read_parquet('{PARQUET_PATH}') WHERE {where_sql} GROUP BY {group_by}"
            for m in metrics
        ]
        sql = " UNION ALL ".join(subs)
        if a.get("k"):
            sql = f"SELECT * FROM ({sql}) ORDER BY value DESC LIMIT {int(a['k'])}"
        return sql

    # Multiple metrics, no grouping -> single row
    if len(metrics) > 1 and group_by == "none":
        select_parts = [f"{_agg_expr(m, agg)} AS {m}" for m in metrics]
        select_parts.append("SUM(g) AS games_played")
        return (
            f"SELECT {', '.join(select_parts)} "
            f"FROM read_parquet('{PARQUET_PATH}') WHERE {where_sql}"
        )

    # Single metric path
    metric = metrics[0]
    if group_by == "none":
        return (
            f"SELECT {_agg_expr(metric, agg)} AS {metric}, SUM(g) AS games_played "
            f"FROM read_parquet('{PARQUET_PATH}') WHERE {where_sql}"
        )

    # Single metric with grouping
    sql = (
        f"SELECT {group_by} AS {group_by}, {_agg_expr(metric, agg)} AS value, "
        f"SUM(g) AS games_played "
        f"FROM read_parquet('{PARQUET_PATH}') WHERE {where_sql} GROUP BY {group_by}"
    )
    if a.get("k"):
        sql += f" ORDER BY value DESC LIMIT {int(a['k'])}"
    return sql

def _execute(sql: str, params: List[Any]) -> List[Dict[str, Any]]:
    cur = _con().execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]

def _to_markdown(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "_No results._"
    headers = list(rows[0].keys())
    lines = ["| " + " | ".join(headers) + " |",
             "| " + " | ".join(["---"] * len(headers)) + " |"]
    for r in rows:
        lines.append("| " + " | ".join(str(r[h]) for h in headers) + " |")
    return "\n".join(lines)

# --- Core callable with explicit kwargs (StructuredTool friendly) ---
def run_player_stats_op(
    season: Optional[str] = None,
    metric: Optional[str] = None,
    metrics: Optional[List[str]] = None,
    agg: str = "avg",
    group_by: str = "none", 
    players: Optional[List[str]] = None,
    teams: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    k: Optional[int] = None,
) -> str:
    if isinstance(group_by, list):
        group_by = group_by[0] if group_by else "none"

    season = season or DEFAULT_SEASON

    # Default to COUNT(*) when no metric(s) provided
    if not metric and not metrics:
        agg = "count"
    metrics_list = _coalesce_metrics(metric, metrics)
    if not metrics_list:
        metrics_list = ["row_count"]  # synthetic label for COUNT(*)

    a: Dict[str, Any] = {
        "season": season,
        "metrics": metrics_list,
        "agg": agg,
        "group_by": group_by,
        "players": players,
        "teams": teams,
        "filters": filters,
        "k": k,
    }
    sql = _build_sql(a)
    params = _build_params(a)
    rows = _execute(sql, params)
    print(sql, rows, params)  # debug
    return _to_markdown(rows)

# --- Structured Tool ---
player_stats_aggregate_tool = StructuredTool.from_function(
    name="player_stats_aggregate_tool",
    description=(
        "Aggregate player stats from parquet. "
        "Args: season, metric|metrics, agg(avg|sum|min|max|count|median|pNN), "
        "group_by(none|player|team|position), players, teams, filters, k."
    ),
    func=run_player_stats_op,
    args_schema=PlayerStatsAggregateArgs,
)

if __name__ == "__main__":
    """
    Quick manual demo for player_stats_aggregate.
    Run:
        PYTHONPATH=. python src/tools/compute/player_stats.py
    (Optionally) pass a JSON string of args:
        PYTHONPATH=. python src/tools/compute/player_stats.py '{"metric":"pts","agg":"avg","group_by":"none"}'
    """
    import sys, json

    # If user provided a single JSON arg, use it; otherwise show multiple examples.
    if len(sys.argv) > 1:
        try:
            user_args = json.loads(sys.argv[1])
            print("User args:", user_args)
            print(run_player_stats_op(**user_args))  # <-- unpack
            sys.exit(0)
        except Exception as e:
            print(f"Failed to parse JSON args: {e}. Falling back to examples.\n")

    examples: List[Dict[str, Any]] = [
        # 1. League average 3PT% for default season
        {"metric": "three_pct", "agg": "avg", "group_by": "none"},

        # 2. League average FG% and 3PT% (multiple metrics, no grouping -> one row)
        {"metrics": ["fg_pct", "three_pct"], "agg": "avg", "group_by": "none"},

        # 3. Average points per team (grouped)
        {"metric": "pts", "agg": "avg", "group_by": "team", "k": 5},  # top 5 teams by avg pts

        # 4. Percentile example: p90 of assists per player (filter by games played >= 50)
        {"metric": "ast", "agg": "p90", "group_by": "none", "filters": {"g__gte": 50}},

        # 5. Multiple metrics grouped by team (UNION form)
        {"metrics": ["pts", "ast"], "agg": "avg", "group_by": "team", "k": 5},

        # 6. Single player focus (list of players)
        {"metric": "pts", "agg": "avg", "group_by": "none", "players": ["Darius Garland"]},

        # 7. Filter by team exact match
        {"metric": "three_pct", "agg": "avg", "group_by": "none", "teams": ["Cleveland Cavaliers"]},
    ]

    for i, args in enumerate(examples):
        print(f"\n=== Example {i} Args ===")
        print(json.dumps(args, indent=2))
        try:
            out = run_player_stats_op(**args)  # <-- unpack
        except Exception as e:
            out = f"ERROR: {e}"
        print("Result:\n" + out)
