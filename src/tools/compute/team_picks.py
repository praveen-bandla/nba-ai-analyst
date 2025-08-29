# tools/compute/sql/team_picks_tool.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
import duckdb
from pydantic import BaseModel, Field, field_validator
from langchain.tools import StructuredTool

# Parquet with columns: team, pick_year (int), pick_round ("First"/"Second"), details
PARQUET_PATH = "data/parquet/team_picks.parquet"

# Cached DuckDB connection
_CONN: duckdb.DuckDBPyConnection | None = None
def _con() -> duckdb.DuckDBPyConnection:
    global _CONN
    if _CONN is None:
        _CONN = duckdb.connect(database=":memory:")
    return _CONN

# ---------- Helpers ----------
def _season_to_year(season: str) -> Optional[int]:
    """
    Convert '2026-27' -> 2026 ; '2026' -> 2026 ; else None.
    Draft (pick) year == first part of NBA season span.
    """
    if not season:
        return None
    season = season.strip()
    if "-" in season:
        head = season.split("-", 1)[0]
        return int(head) if head.isdigit() else None
    if season.isdigit():
        return int(season)
    return None

def _normalize_round(r: Optional[str]) -> Optional[str]:
    if not r:
        return None
    r_lower = str(r).lower()
    if r_lower in {"1", "first", "rd1"}:
        return "First"
    if r_lower in {"2", "second", "rd2"}:
        return "Second"
    return None  # ignore unknown

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

# ---------- Args Schema ----------
_ALLOWED_GROUP_BY = {"none", "team", "year", "round"}
_ALLOWED_AGG = {"count", "none"}

class TeamPicksAggregateArgs(BaseModel):
    # Flexible ways to specify years
    season: Optional[str] = Field(default=None, description="Season like '2026-27' (first year used as pick_year)")
    year: Optional[int] = Field(default=None, description="Single draft year (e.g. 2026)")
    start_year: Optional[int] = Field(default=None, description="Start of inclusive year range")
    end_year: Optional[int] = Field(default=None, description="End of inclusive year range")
    years: Optional[List[int]] = Field(default=None, description="Explicit list of draft years")
    teams: Optional[List[str]] = Field(default=None, description="Filter to these team strings (exact match)")
    pick_round: Optional[str] = Field(default=None, description="Round filter: First|Second (case-insensitive or 1/2)")
    agg: str = Field(default="count", description="count|none (none = list raw rows)")
    group_by: Any = Field(default="team", description="none|team|year|round")
    k: Optional[int] = Field(default=None, description="Top-k limit for grouped results")
    limit: Optional[int] = Field(default=None, description="Limit for raw listing (agg=none)")
    filters: Optional[Dict[str, Any]] = Field(default=None, description="Extra filters: details__like substring")
    
    @field_validator("group_by", mode="before")
    @classmethod
    def _coerce_group_by(cls, v):
        if isinstance(v, list):
            return v[0] if v else "team"
        return v
    
    @field_validator("group_by")
    @classmethod
    def _validate_group_by(cls, v: str):
        if v not in _ALLOWED_GROUP_BY:
            raise ValueError(f"group_by must be one of {_ALLOWED_GROUP_BY}")
        return v
    
    @field_validator("agg")
    @classmethod
    def _validate_agg(cls, v: str):
        if v not in _ALLOWED_AGG:
            raise ValueError(f"agg must be one of {_ALLOWED_AGG}")
        return v
    
    @field_validator("pick_round", mode="before")
    @classmethod
    def _norm_round(cls, v):
        return _normalize_round(v)

# ---------- Core SQL Builder ----------
def _years_clause(a: TeamPicksAggregateArgs, params: List[Any]) -> str:
    year_filters: List[str] = []
    # Derive a single year from season if provided and no explicit year/years range
    if a.season and not any([a.year, a.years, a.start_year, a.end_year]):
        yr = _season_to_year(a.season)
        if yr:
            a.year = yr
    
    if a.years:
        placeholders = ",".join(["?"] * len(a.years))
        year_filters.append(f"pick_year IN ({placeholders})")
        params.extend(a.years)
    elif a.year is not None:
        year_filters.append("pick_year = ?")
        params.append(a.year)
    else:
        if a.start_year is not None:
            year_filters.append("pick_year >= ?")
            params.append(a.start_year)
        if a.end_year is not None:
            year_filters.append("pick_year <= ?")
            params.append(a.end_year)
    return " AND ".join(year_filters) if year_filters else ""

def _where(a: TeamPicksAggregateArgs) -> (str, List[Any]):
    params: List[Any] = []
    clauses: List[str] = []
    
    yc = _years_clause(a, params)
    if yc:
        clauses.append(yc)
    
    if a.teams:
        # Minimal adjustment: dataset stores rows as "<Team> Future NBA Draft Picks"
        teams_full = [
            t if t.lower().endswith("future nba draft picks")
            else f"{t} Future NBA Draft Picks"
            for t in a.teams
        ]
        placeholders = ",".join(["?"] * len(teams_full))
        clauses.append(f"team IN ({placeholders})")
        params.extend(teams_full)
    
    if a.pick_round:
        clauses.append("pick_round = ?")
        params.append(a.pick_round)
    
    # Extra filters
    if a.filters:
        like_val = a.filters.get("details__like")
        if like_val:
            clauses.append("LOWER(details) LIKE ?")
            params.append(f"%{like_val.lower()}%")
    
    where_sql = " AND ".join(clauses) if clauses else "TRUE"
    return where_sql, params

def _build_sql(a: TeamPicksAggregateArgs) -> (str, List[Any]):
    where_sql, params = _where(a)
    
    # Raw listing
    if a.agg == "none":
        select_cols = "team, pick_year, pick_round, details"
        order = ""
        if a.group_by == "team":
            order = "ORDER BY team, pick_year, pick_round"
        elif a.group_by == "year":
            order = "ORDER BY pick_year, team, pick_round"
        elif a.group_by == "round":
            order = "ORDER BY pick_round, pick_year, team"
        else:
            order = "ORDER BY pick_year, team, pick_round"
        sql = f"""
        SELECT {select_cols}
        FROM read_parquet('{PARQUET_PATH}')
        WHERE {where_sql}
        {order}
        """
        if a.limit:
            sql += f" LIMIT {int(a.limit)}"
        return sql, params
    
    # agg == count
    if a.group_by == "team":
        sql = f"""
        SELECT team, COUNT(*) AS value
        FROM read_parquet('{PARQUET_PATH}')
        WHERE {where_sql}
        GROUP BY team
        ORDER BY value DESC
        """
    elif a.group_by == "year":
        sql = f"""
        SELECT pick_year AS year, COUNT(*) AS value
        FROM read_parquet('{PARQUET_PATH}')
        WHERE {where_sql}
        GROUP BY pick_year
        ORDER BY value DESC
        """
    elif a.group_by == "round":
        sql = f"""
        SELECT pick_round AS round, COUNT(*) AS value
        FROM read_parquet('{PARQUET_PATH}')
        WHERE {where_sql}
        GROUP BY pick_round
        ORDER BY value DESC
        """
    else:  # none
        sql = f"""
        SELECT COUNT(*) AS value
        FROM read_parquet('{PARQUET_PATH}')
        WHERE {where_sql}
        """
    if a.k and a.group_by in {"team", "year", "round"}:
        sql += f" LIMIT {int(a.k)}"
    return sql, params

def run_team_picks_agg(
    season: Optional[str] = None,
    year: Optional[int] = None,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    years: Optional[List[int]] = None,
    teams: Optional[List[str]] = None,
    pick_round: Optional[str] = None,
    agg: str = "count",
    group_by: str = "team",
    k: Optional[int] = None,
    limit: Optional[int] = None,
    filters: Optional[Dict[str, Any]] = None,
) -> str:
    args = TeamPicksAggregateArgs(
        season=season,
        year=year,
        start_year=start_year,
        end_year=end_year,
        years=years,
        teams=teams,
        pick_round=pick_round,
        agg=agg,
        group_by=group_by,
        k=k,
        limit=limit,
        filters=filters,
    )
    sql, params = _build_sql(args)
    cur = _con().execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]

    print(sql, cols, rows)
    return _markdown(rows)

team_picks_aggregate_tool = StructuredTool.from_function(
    name="team_picks_aggregate_tool",
    description=(
        "Analyze future draft picks. Args: season ('2026-27'), year, start_year, end_year, years(list), "
        "teams(list), pick_round(First|Second|1|2), agg(count|none), group_by(team|year|round|none), "
        "k (limit for grouped), limit (row cap for agg=none), filters (details__like substring)."
    ),
    func=run_team_picks_agg,
    args_schema=TeamPicksAggregateArgs,
)

if __name__ == "__main__":
    tests = [
        {"season": "2026-27", "group_by": "team", "agg": "count"},
        {"year": 2028, "group_by": "round", "agg": "count"},
        {"start_year": 2026, "end_year": 2028, "agg": "none", "group_by": "year", "limit": 10},
        {"years": [2026, 2027], "pick_round": "First", "group_by": "year", "agg": "count"},
        {"year": 2026, "agg": "none", "group_by": "team", "limit": 5},
    ]
    for t in tests:
        print("\nArgs:", t)
        try:
            print(run_team_picks_agg(**t))
        except Exception as e:
            print("Error:", e)
