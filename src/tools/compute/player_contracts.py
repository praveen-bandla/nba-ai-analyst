# src/tools/compute/player_contracts.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
import duckdb
from pydantic import BaseModel, Field, field_validator
from src.capabilities.team_abbrev import TEAM_NAME_TO_ABBR
from langchain.tools import StructuredTool

PARQUET_PATH = "data/parquet/player_contracts.parquet"
DEFAULT_SEASON = "2024-25"

# cache DuckDB connection
_con_cache: duckdb.DuckDBPyConnection | None = None
def _con() -> duckdb.DuckDBPyConnection:
    global _con_cache
    if _con_cache is None:
        _con_cache = duckdb.connect(database=":memory:")
    return _con_cache

class ContractsAggArgs(BaseModel):
    # NOTE: season is ONLY used to choose the correct salary_<YYYY_YY> column.
    season: Optional[str] = Field(default=None, description="Season label (e.g. 2026-27) used to pick salary_<YYYY_YY> column")
    players: Optional[List[str]] = Field(default=None, description="Filter to these player names")
    teams: Optional[List[str]] = Field(default=None, description="Filter to these teams (abbreviations in your parquet)")
    metric: str = Field(default="salary", description="Either 'salary' or explicit salary_<YYYY_YY> or another numeric column")
    agg: str = Field(default="max", description="Aggregation: max|min|sum|avg|count")
    group_by: str = Field(default="none", description="none|player|team")
    k: Optional[int] = Field(default=None, description="Top-k after grouping")
    filters: Optional[Dict[str, Any]] = Field(default=None)

    @field_validator("group_by", mode="before")
    @classmethod
    def _coerce_group_by(cls, v):
        if isinstance(v, list):
            return v[0] if v else "none"
        return v

    @field_validator("agg")
    @classmethod
    def _agg_ok(cls, v: str) -> str:
        v = v.lower()
        if v in {"max","min","sum","avg","count"}:
            return v
        raise ValueError("agg must be one of max|min|sum|avg|count")

    @field_validator("group_by")
    @classmethod
    def _group_ok(cls, v: str) -> str:
        if v in {"none","player","team"}:
            return v
        raise ValueError("group_by must be none|player|team")

    @field_validator("teams", mode="before")
    @classmethod
    def _normalize_teams(cls, v):
        if not v:
            return v
        out = []
        for t in v:
            key = t.lower()
            out.append(TEAM_NAME_TO_ABBR.get(key, t))
        return out

# ---- NEW helpers for dynamic metric resolution (season -> salary column) ----
_SCHEMA_COLS: List[str] | None = None
def _schema_cols() -> List[str]:
    global _SCHEMA_COLS
    if _SCHEMA_COLS is None:
        cur = _con().execute(f"SELECT * FROM read_parquet('{PARQUET_PATH}') LIMIT 1")
        _SCHEMA_COLS = [d[0] for d in cur.description]
    return _SCHEMA_COLS

def _season_to_salary_col(season: str) -> str:
    # '2026-27' -> salary_2026_27
    return f"salary_{season.replace('-', '_')}"

def _resolve_metric(a: ContractsAggArgs) -> str:
    cols = _schema_cols()
    # Explicit salary_<...>
    if a.metric.startswith("salary_"):
        return a.metric if a.metric in cols else a.metric  # will fail later if missing
    # Generic 'salary' + season
    if a.metric == "salary" and a.season:
        cand = _season_to_salary_col(a.season)
        if cand in cols:
            return cand
    # Fallback: if generic 'salary' with no season, pick latest salary_ column lexicographically
    if a.metric == "salary":
        salary_cols = sorted([c for c in cols if c.startswith("salary_")], reverse=True)
        if salary_cols:
            return salary_cols[0]
    return a.metric  # may or may not exist; DuckDB will error if invalid

def _agg_expr(metric: str, agg: str) -> str:
    if agg == "count":
        return "COUNT(*)"
    return f"{agg.upper()}({metric})"

def _build_where(a: ContractsAggArgs) -> tuple[str, List[Any]]:
    """
    IMPORTANT: The contracts parquet has NO 'season' column, so we do NOT filter by season.
    Season only selects which salary_<YYYY_YY> column to aggregate.
    """
    clauses: List[str] = []
    params: List[Any] = []

    if a.players:
        placeholders = ",".join(["?"] * len(a.players))
        clauses.append(f"name IN ({placeholders})")
        params.extend(a.players)

    if a.teams:
        placeholders = ",".join(["?"] * len(a.teams))
        clauses.append(f"team IN ({placeholders})")
        params.extend(a.teams)

    op_map = {"gte": ">=", "lte": "<=", "gt": ">", "lt": "<", "eq": "=", "ne": "!="}
    if a.filters:
        for key, val in a.filters.items():
            if val is None:
                continue
            if "__" in key:
                col, suf = key.split("__", 1)
                if suf in op_map:
                    clauses.append(f"{col} {op_map[suf]} ?")
                    params.append(val)
            else:
                clauses.append(f"{key} = ?")
                params.append(val)

    where_sql = " AND ".join(clauses) if clauses else "TRUE"
    return where_sql, params

def _build_sql(a: ContractsAggArgs) -> tuple[str, List[Any]]:
    # Resolve metric before SQL
    resolved_metric = _resolve_metric(a)
    a.metric = resolved_metric
    # Auto-upgrade grouping: if multiple players requested and no group specified, group by player.
    if a.group_by == "none" and a.players and len(a.players) > 1:
        a.group_by = "player"  # logical keyword
    where_sql, params = _build_where(a)
    agg_expr = _agg_expr(a.metric, a.agg)

    # Map logical group_by keyword 'player' to physical column 'name'
    group_col_physical = "name" if a.group_by == "player" else a.group_by

    # Always include note (and team/name where possible)
    if a.group_by == "none":
        # Single aggregate row (may represent one or many players). We still surface a representative
        # name/team/note via first().
        sql = f"""
        SELECT
            {agg_expr} AS value,
            first(name)  AS name,
            first(team)  AS team,
            first(note)  AS note
        FROM read_parquet('{PARQUET_PATH}')
        WHERE {where_sql}
        """
        return sql, params

    # Grouped case: include group column + value + team (if grouping by player) + note
    select_extra = ""
    if a.group_by == "player":  # grouping by logical player -> name
        select_extra = ", first(team) AS team, first(note) AS note"
    elif a.group_by == "team":
        select_extra = ", first(note) AS note"
    else:
        # Unexpected group_by (should only be player|team); still attempt note
        select_extra = ", first(note) AS note"

    sql = f"""
    SELECT
        {group_col_physical} AS {a.group_by},
        {agg_expr} AS value
        {select_extra}
    FROM read_parquet('{PARQUET_PATH}')
    WHERE {where_sql}
    GROUP BY {group_col_physical}
    ORDER BY value DESC
    """
    if a.k:
        sql += f" LIMIT {int(a.k)}"
    return sql, params

def _execute(sql: str, params: List[Any]) -> List[Dict[str, Any]]:
    cur = _con().execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return [{cols[i]: row[i] for i in range(len(cols))} for row in rows]

def _to_markdown(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "_No contract results._"
    headers = list(rows[0].keys())
    lines = ["| " + " | ".join(headers) + " |",
             "| " + " | ".join(["---"] * len(headers)) + " |"]
    for r in rows:
        lines.append("| " + " | ".join(str(r[h]) for h in headers) + " |")
    return "\n".join(lines)

def run_contracts_agg(
    season: Optional[str] = None,
    players: Optional[List[str]] = None,
    teams: Optional[List[str]] = None,
    metric: str = "salary",
    agg: str = "max",
    group_by: str = "none",
    k: Optional[int] = None,
    filters: Optional[Dict[str, Any]] = None,
) -> str:
    args = ContractsAggArgs(
        season=season,
        players=players,
        teams=teams,
        metric=metric,
        agg=agg,
        group_by=group_by,
        k=k,
        filters=filters,
    )
    sql, params = _build_sql(args)
    rows = _execute(sql, params)
    # Optional: if grouping by player and you really wanted raw contract lines, you'd switch tools.
    return _to_markdown(rows)

contracts_aggregate_tool = StructuredTool.from_function(
    name="contracts_aggregate",
    description=(
        "Aggregate contract salary columns. Args: season (used to pick salary_<YYYY_YY>), players, teams, "
        "metric('salary' or explicit column), agg(max|min|sum|avg|count), group_by(none|player|team), k, filters(col__gte style)."
    ),
    func=run_contracts_agg,
    args_schema=ContractsAggArgs,
)

if __name__ == "__main__":
    """
    Simple CLI test cases demonstrating run_contracts_agg usage.
    Run:
        PYTHONPATH=. python src/tools/compute/player_contracts.py
    """
    tests = [
        {
            "desc": "Single player latest salary (auto-resolve season column)",
            "kwargs": {"players": ["Stephen Curry"], "metric": "salary", "agg": "max", "group_by": "none"},
        },
        {
            "desc": "Multiple players grouped by player (explicit season selects salary_<YYYY_YY>)",
            "kwargs": {"season": "2026-27", "players": ["Stephen Curry", "Klay Thompson"], "metric": "salary", "agg": "max", "group_by": "player"},
        },
        {
            "desc": "Team aggregate (sum of salaries for team)",
            "kwargs": {"season": "2025-26", "teams": ["GSW"], "metric": "salary", "agg": "sum", "group_by": "team"},
        },
        {
            "desc": "Count of contract rows for a team",
            "kwargs": {"teams": ["LAL"], "metric": "salary", "agg": "count", "group_by": "team"},
        },
        {
            "desc": "Explicit salary column (fallback if season column missing)",
            "kwargs": {"metric": "salary_2027_28", "agg": "max", "group_by": "none", "players": ["Luka Doncic"]},
        },
    ]

    for t in tests:
        print("\n=== Test:", t["desc"], "===")
        try:
            print(run_contracts_agg(**t["kwargs"]))
        except Exception as e:
            print("Error:", e)


