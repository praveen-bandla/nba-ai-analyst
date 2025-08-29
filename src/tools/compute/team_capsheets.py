# tools/compute/team_capsheets.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
import duckdb
from pydantic import BaseModel, Field, field_validator
from langchain.tools import StructuredTool

PARQUET_PATH = "data/parquet/team_capsheets.parquet"

# Single cached DuckDB connection
_conn: duckdb.DuckDBPyConnection | None = None
def _con() -> duckdb.DuckDBPyConnection:
    global _conn
    if _conn is None:
        _conn = duckdb.connect(database=":memory:")
    return _conn

# Column cache
_COLS: List[str] | None = None
def _cols() -> List[str]:
    global _COLS
    if _COLS is None:
        cur = _con().execute(f"SELECT * FROM read_parquet('{PARQUET_PATH}') LIMIT 1")
        _COLS = [d[0] for d in cur.description]
    return _COLS

# Map any salary/cap-ish metric hint to base 'cap'
_CAP_SYNONYMS = {"cap", "salary", "salary_cap", "cap_space", "total_salary"}

def _season_to_col(season: Optional[str]) -> Optional[str]:
    if not season:
        return None
    return f"cap_{season.replace('-', '_')}"

def _pick_cap_column(season: Optional[str], metric: Optional[str]) -> str:
    cols = _cols()
    # Explicit existing column
    if metric and metric in cols:
        return metric
    # Normalize metric base
    base = (metric or "cap").lower()
    if base in _CAP_SYNONYMS:
        # Try season-specific
        if season:
            cand = _season_to_col(season)
            if cand and cand in cols:
                return cand
        # Fallback: latest (max) cap_YYYY_YY column
        cap_cols = sorted([c for c in cols if c.startswith("cap_")])
        if cap_cols:
            # If season requested but not present, attempt nearest (by start year)
            if season:
                try:
                    target = int(season.split("-")[0])
                    by_year = []
                    for c in cap_cols:
                        try:
                            y = int(c.split("_")[1])
                            by_year.append((abs(y - target), y, c))
                        except:
                            continue
                    by_year.sort()
                    return by_year[0][2]
                except:
                    pass
            return cap_cols[-1]
    # As last resort, raise (will surface a clear error)
    raise ValueError(f"Could not resolve cap column for season='{season}' metric='{metric}'")

def _markdown(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "_No results._"
    headers = list(rows[0].keys())
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for r in rows:
        lines.append("| " + " | ".join(str(r[h]) for h in headers) + " |")
    return "\n".join(lines)

class TeamCapsArgs(BaseModel):
    season: Optional[str] = Field(default=None, description="Season like 2026-27 (used to select cap_<YYYY_YY> column)")
    metric: Optional[str] = Field(default="cap", description="Metric hint or explicit column (cap_YYYY_YY). Hints: cap, salary, cap_space")
    group_by: str = Field(default="team", description="team or none")
    agg: str = Field(default="max", description="Aggregation for group_by=none: max|min|avg|sum|count")
    k: Optional[int] = Field(default=None, description="Top-k teams (only when group_by=team)")
    teams: Optional[List[str]] = Field(default=None, description="Optional filter list of team names")
    filters: Optional[Dict[str, Any]] = Field(default=None, description="Additional filters col=value or col__gte style")

    @field_validator("group_by", mode="before")
    @classmethod
    def _coerce_group(cls, v):
        if isinstance(v, list):
            return v[0] if v else "team"
        return v

    @field_validator("group_by")
    @classmethod
    def _check_group(cls, v):
        if v not in {"team", "none"}:
            raise ValueError("group_by must be 'team' or 'none'")
        return v

    @field_validator("agg")
    @classmethod
    def _check_agg(cls, v):
        v = v.lower()
        if v in {"max","min","avg","sum","count"}:
            return v
        raise ValueError("agg must be one of max|min|avg|sum|count")

def _build_where(a: TeamCapsArgs) -> tuple[str, List[Any]]:
    clauses: List[str] = []
    params: List[Any] = []
    if a.teams:
        placeholders = ",".join(["?"] * len(a.teams))
        clauses.append(f"team IN ({placeholders})")
        params.extend(a.teams)
    if a.filters:
        op_map = {"gte": ">=", "lte": "<=", "gt": ">", "lt": "<", "eq": "=", "ne": "!="}
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
    return (" AND ".join(clauses)) if clauses else "TRUE", params

def run_team_capsheets(
    season: Optional[str] = None,
    metric: Optional[str] = "cap",
    group_by: str = "team",
    agg: str = "max",
    k: Optional[int] = None,
    teams: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
) -> str:
    args = TeamCapsArgs(
        season=season,
        metric=metric,
        group_by=group_by,
        agg=agg,
        k=k,
        teams=teams,
        filters=filters,
    )
    cap_col = _pick_cap_column(args.season, args.metric)
    where_sql, params = _build_where(args)
    con = _con()

    if args.group_by == "team":
        # Direct values per team (no aggregation over single value)
        sql = f"""
        SELECT team, {cap_col} AS value
        FROM read_parquet('{PARQUET_PATH}')
        WHERE {where_sql}
        ORDER BY value DESC
        """
        if args.k:
            sql += f" LIMIT {int(args.k)}"
    else:
        # Aggregate across all teams
        if args.agg == "count":
            agg_expr = "COUNT(*)"
        else:
            agg_expr = f"{args.agg.upper()}({cap_col})"
        # capture team with max when agg=max (optional)
        extra_cols = ""
        if args.agg == "max":
            extra_cols = f", (SELECT team FROM read_parquet('{PARQUET_PATH}') ORDER BY {cap_col} DESC LIMIT 1) AS top_team"
        sql = f"""
        SELECT {agg_expr} AS value, '{cap_col}' AS metric_col{extra_cols}
        FROM read_parquet('{PARQUET_PATH}')
        WHERE {where_sql}
        """

    cur = con.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]
    return _markdown(rows)

team_capsheets_aggregate_tool = StructuredTool.from_function(
    name="team_capsheets_aggregate",
    description="Team cap sheet aggregation. Args: season, metric (cap|salary|cap_space or cap_YYYY_YY), group_by(team|none), agg (for none), k, teams filter.",
    func=run_team_capsheets,
    args_schema=TeamCapsArgs,
)

if __name__ == "__main__":
    tests = [
        {"season": "2026-27", "metric": "cap_space", "group_by": "team", "k": 5},
        {"season": "2026-27", "metric": "cap", "group_by": "team", "k": 3},
        {"season": "2026-27", "metric": "cap", "group_by": "none", "agg": "max"},
    ]
    for t in tests:
        print("\nArgs:", t)
        try:
            print(run_team_capsheets(**t))
        except Exception as e:
            print("ERROR:", e)
