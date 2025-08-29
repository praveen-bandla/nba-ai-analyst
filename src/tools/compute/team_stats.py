# tools/compute/sql/team_stats_tool.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
import duckdb
from pydantic import BaseModel, Field, field_validator
from langchain.tools import StructuredTool

PARQUET_PATH = "data/parquet/team_stats.parquet"
DEFAULT_SEASON = "2024-25"

# --- DuckDB connection cache ---
_duck_con: duckdb.DuckDBPyConnection | None = None
def _con() -> duckdb.DuckDBPyConnection:
    global _duck_con
    if _duck_con is None:
        _duck_con = duckdb.connect(database=":memory:")
    return _duck_con

# --- Schema (lazy) ---
_SCHEMA_COLS: List[str] | None = None
_NUMERIC_COLS: List[str] | None = None
def _schema_cols() -> List[str]:
    global _SCHEMA_COLS, _NUMERIC_COLS
    if _SCHEMA_COLS is None:
        cur = _con().execute(f"SELECT * FROM read_parquet('{PARQUET_PATH}') LIMIT 1")
        _SCHEMA_COLS = [d[0] for d in cur.description]
        ignore = {"team", "season", "rk"}
        _NUMERIC_COLS = [c for c in _SCHEMA_COLS if c not in ignore]
    return _SCHEMA_COLS

def _numeric_cols() -> List[str]:
    if _NUMERIC_COLS is None:
        _schema_cols()
    return _NUMERIC_COLS  # type: ignore

# --- Args schema ---
class TeamStatsAggregateArgs(BaseModel):
    season: Optional[str] = Field(default=DEFAULT_SEASON)
    metric: Optional[str] = Field(default=None, description="Single numeric stat column")
    metrics: Optional[List[str]] = Field(default=None, description="Multiple numeric stat columns")
    agg: str = Field(default="avg", description="avg|sum|min|max|count|median|pNN")
    group_by: Any = Field(default="none", description="none|team")
    teams: Optional[List[str]] = Field(default=None, description="Exact team names (trailing * will be auto-handled)")
    filters: Optional[Dict[str, Any]] = Field(default=None, description="Column filters col=value or col__gte style")
    k: Optional[int] = Field(default=None, description="Top-k after grouping / union")
    include_league_average: bool = Field(default=False, description="Include 'League Average' row(s) when filtering")
    
    @field_validator("group_by", mode="before")
    @classmethod
    def _coerce_group(cls, v):
        if isinstance(v, list):
            return v[0] if v else "none"
        return v

    @field_validator("agg")
    @classmethod
    def _agg_ok(cls, v: str):
        v = v.lower()
        if v in {"avg","sum","min","max","count","median"}:
            return v
        if v.startswith("p") and v[1:].isdigit():
            q = int(v[1:])
            if 0 < q < 100:
                return v
        raise ValueError("agg must be avg|sum|min|max|count|median|pNN")

    @field_validator("metrics", mode="after")
    def _validate_metrics(cls, v, info):
        if not v:
            return v
        allowed = set(_numeric_cols())
        bad = [m for m in v if m not in allowed]
        if bad:
            raise ValueError(f"Unknown metrics {bad}. Allowed examples: {sorted(list(allowed))[:12]} ...")
        return v

    @field_validator("metric", mode="after")
    def _validate_metric(cls, v, info):
        if v and v not in _numeric_cols():
            raise ValueError(f"Unknown metric '{v}'")
        return v

# --- Helpers ---
def _coalesce_metrics(metric: Optional[str], metrics: Optional[List[str]]) -> List[str]:
    if metrics:
        return list(dict.fromkeys(metrics))
    if metric:
        return [metric]
    return []  # triggers default COUNT path

def _agg_expr(col: str, agg: str) -> str:
    a = agg.lower()
    if a == "count":
        return "COUNT(*)"
    if a == "median":
        return f"median({col})"
    if a.startswith("p"):
        # percentile
        q = int(a[1:]) / 100.0
        return f"quantile({col}, {q})"
    return f"{a.upper()}({col})"

def _build_where(a: Dict[str, Any]) -> str:
    clauses: List[str] = []
    # season filter
    if a.get("season"):
        clauses.append("season = ?")
    # team filter (handle star)
    teams = a.get("teams")
    if teams:
        expanded: List[str] = []
        for t in teams:
            expanded.append(t)
            if not t.endswith("*"):
                expanded.append(t + "*")
        placeholders = ",".join(["?"] * len(expanded))
        clauses.append(f"team IN ({placeholders})")
        a["_expanded_teams"] = expanded  # stash for params
    # exclude league average unless included
    if not a.get("include_league_average"):
        clauses.append("team <> 'League Average'")
    # generic filters col / col__op
    filt = a.get("filters") or {}
    op_map = {"gte": ">=", "lte": "<=", "gt": ">", "lt": "<", "eq": "=", "ne": "!="}
    for k, v in filt.items():
        if v is None:
            continue
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
    if a.get("teams"):
        params.extend(a["_expanded_teams"])
    filt = a.get("filters") or {}
    for k, v in filt.items():
        if v is None:
            continue
        params.append(v)
    return params

def _build_sql(a: Dict[str, Any]) -> str:
    where_sql = _build_where(a)
    metrics = a["metrics"]
    agg = a["agg"]
    group_by = a["group_by"]

    # Default row count
    if not metrics:
        a["metrics"] = ["row_count"]
        metrics = a["metrics"]
        agg = "count"

    # Single metric
    if len(metrics) == 1:
        metric = metrics[0]
        if metric == "row_count":
            if group_by == "none":
                return f"SELECT COUNT(*) AS row_count FROM read_parquet('{PARQUET_PATH}') WHERE {where_sql}"
            sql = (
                f"SELECT team AS team, COUNT(*) AS value "
                f"FROM read_parquet('{PARQUET_PATH}') WHERE {where_sql} GROUP BY team"
            )
            if a.get("k"):
                sql += f" ORDER BY value DESC LIMIT {int(a['k'])}"
            return sql
        if group_by == "none":
            return (
                f"SELECT {_agg_expr(metric, agg)} AS {metric} "
                f"FROM read_parquet('{PARQUET_PATH}') WHERE {where_sql}"
            )
        sql = (
            f"SELECT team AS team, {_agg_expr(metric, agg)} AS value "
            f"FROM read_parquet('{PARQUET_PATH}') WHERE {where_sql} GROUP BY team"
        )
        if a.get("k"):
            sql += f" ORDER BY value DESC LIMIT {int(a['k'])}"
        return sql

    # Multi-metric
    if group_by == "none":
        selects = [f"{_agg_expr(m, agg)} AS {m}" for m in metrics]
        return f"SELECT {', '.join(selects)} FROM read_parquet('{PARQUET_PATH}') WHERE {where_sql}"

    # Multi metrics + group_by team: UNION rows (team, value, metric)
    subs = [
        f"SELECT team AS team, {_agg_expr(m, agg)} AS value, '{m}' AS metric "
        f"FROM read_parquet('{PARQUET_PATH}') WHERE {where_sql} GROUP BY team"
        for m in metrics
    ]
    union_sql = " UNION ALL ".join(subs)
    if a.get("k"):
        union_sql = f"SELECT * FROM ({union_sql}) ORDER BY value DESC LIMIT {int(a['k'])}"
    return union_sql

def _execute(sql: str, params: List[Any]) -> List[Dict[str, Any]]:
    cur = _con().execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]

def _to_markdown(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "_No team stat results._"
    headers = list(rows[0].keys())
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for r in rows:
        lines.append("| " + " | ".join(str(r[h]) for h in headers) + " |")
    return "\n".join(lines)

# --- Core callable ---
def run_team_stats_op(
    season: Optional[str] = DEFAULT_SEASON,
    metric: Optional[str] = None,
    metrics: Optional[List[str]] = None,
    agg: str = "avg",
    group_by: str = "none",
    teams: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    k: Optional[int] = None,
    include_league_average: bool = False,
) -> str:
    if isinstance(group_by, list):
        group_by = group_by[0] if group_by else "none"
    # Ensure schema loaded (validations rely on it)
    _schema_cols()
    args_obj = TeamStatsAggregateArgs(
        season=season,
        metric=metric,
        metrics=metrics,
        agg=agg,
        group_by=group_by,
        teams=teams,
        filters=filters,
        k=k,
        include_league_average=include_league_average,
    )
    metrics_list = _coalesce_metrics(args_obj.metric, args_obj.metrics)
    a: Dict[str, Any] = {
        "season": args_obj.season,
        "metrics": metrics_list,
        "agg": args_obj.agg,
        "group_by": args_obj.group_by,
        "teams": args_obj.teams,
        "filters": args_obj.filters,
        "k": args_obj.k,
        "include_league_average": args_obj.include_league_average,
    }
    sql = _build_sql(a)
    params = _build_params(a)
    rows = _execute(sql, params)
    return _to_markdown(rows)

# --- Structured Tool ---
team_stats_aggregate_tool = StructuredTool.from_function(
    name="team_stats_aggregate_tool",
    description=(
        "Aggregate team stats. Args: season, metric|metrics (e.g. pts, ast, fg_pct, three_pct, trb), "
        "agg(avg|sum|min|max|count|median|pNN), group_by(none|team), teams(list), filters(col or col__gte), "
        "k(top-k), include_league_average(bool). If no metric(s) given returns row_count."
    ),
    func=run_team_stats_op,
    args_schema=TeamStatsAggregateArgs,
)

if __name__ == "__main__":
    import json
    examples = [
        {"metric": "pts", "agg": "avg", "group_by": "none"},
        {"metrics": ["pts","ast","three_pct"], "agg": "avg", "group_by": "none"},
        {"metric": "pts", "agg": "avg", "group_by": "team", "k": 5},
        {"metric": "three_pct", "agg": "avg", "group_by": "none", "teams": ["Cleveland Cavaliers"]},
        {"metrics": ["pts","ast"], "agg": "avg", "group_by": "team", "k": 5},
        {"agg": "count", "group_by": "team", "k": 5},  # implicit count
        {"metric": "pts", "agg": "p90", "group_by": "none"},
        {"metric": "fg_pct", "agg": "median", "group_by": "team", "k": 5},
    ]
    for i, ex in enumerate(examples):
        print(f"\n=== Example {i} ===")
        print(json.dumps(ex, indent=2))
        try:
            print(run_team_stats_op(**ex))
        except Exception as e:
            print(f"Error: {e}")