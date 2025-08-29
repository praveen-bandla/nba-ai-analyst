# src/parquet_builders/player_stats.py
import argparse
from pathlib import Path
import duckdb
from config.settings import DATASETS, PARQUET_FOLDERS

def build_player_stats_parquet(season: str) -> Path:
    """
    Read the raw 'all_player_stats_by_team.csv' and write a single Parquet file,
    adding a 'season' column. No custom formatters; DuckDB handles types.
    """
    src_csv = Path(DATASETS["player_stats"])
    out_dir = Path(PARQUET_FOLDERS["player_stats"])
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "player_stats.parquet"

    con = duckdb.connect()  # in-memory
    con.execute(f"""
    COPY (
      SELECT
        CAST(Rk AS INT)                 AS rk,
        Player                           AS player,
        "Player-additional"              AS player_id,
        Team                             AS team,
        '{season}'                       AS season,

        TRY_CAST(Age AS DOUBLE)          AS age,
        TRY_CAST(G AS INT)               AS g,
        TRY_CAST(GS AS INT)              AS gs,
        TRY_CAST(MP AS INT)              AS mp,

        TRY_CAST(FG AS INT)              AS fg,
        TRY_CAST(FGA AS INT)             AS fga,
        TRY_CAST("FG%" AS DOUBLE)        AS fg_pct,

        TRY_CAST("3P" AS INT)            AS three_p,
        TRY_CAST("3PA" AS INT)           AS three_pa,
        TRY_CAST("3P%" AS DOUBLE)        AS three_pct,

        TRY_CAST("2P" AS INT)            AS two_p,
        TRY_CAST("2PA" AS INT)           AS two_pa,
        TRY_CAST("2P%" AS DOUBLE)        AS two_pct,

        TRY_CAST("eFG%" AS DOUBLE)       AS efg_pct,

        TRY_CAST(FT AS INT)              AS ft,
        TRY_CAST(FTA AS INT)             AS fta,
        TRY_CAST("FT%" AS DOUBLE)        AS ft_pct,

        TRY_CAST(ORB AS INT)             AS orb,
        TRY_CAST(DRB AS INT)             AS drb,
        TRY_CAST(TRB AS INT)             AS trb,
        TRY_CAST(AST AS INT)             AS ast,
        TRY_CAST(STL AS INT)             AS stl,
        TRY_CAST(BLK AS INT)             AS blk,
        TRY_CAST(TOV AS INT)             AS tov,
        TRY_CAST(PF AS INT)              AS pf,
        TRY_CAST(PTS AS INT)             AS pts,
        TRY_CAST("Trp-Dbl" AS INT)       AS trip_dbl,

        Awards                            AS awards
      FROM read_csv_auto('{src_csv.as_posix()}', header=true)
    )
    TO '{out_path.as_posix()}' (FORMAT PARQUET);
    """)
    con.close()
    print(f"wrote {out_path}")
    return out_path

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", required=True, help="Season label like 2024-25")
    args = ap.parse_args()
    build_player_stats_parquet(args.season)
