# src/parquet_builders/team_stats.py
import argparse
from pathlib import Path
import duckdb
from config.settings import DATASETS, PARQUET_FOLDERS

def build_team_stats_parquet(season: str) -> Path:
    """
    Convert total_team_stats.csv -> data/parquet/team_stats/team_stats.parquet
    - Adds a 'season' column
    - Removes trailing '*' from Team names
    - Fixes leading-dot percentages ('.491' -> '0.491')
    - Keeps everything else simple and typed
    """
    src_csv = Path(DATASETS["team_stats"])
    out_dir = Path(PARQUET_FOLDERS["team_stats"])
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "team_stats.parquet"

    con = duckdb.connect()  # in-memory
    con.execute(f"""
    COPY (
      SELECT
        TRY_CAST(Rk AS INT)                                                    AS rk,
        REGEXP_REPLACE(Team, '\\\\*$', '')                                     AS team,
        '{season}'                                                             AS season,

        TRY_CAST(G   AS INT)                                                   AS g,
        TRY_CAST(MP  AS INT)                                                   AS mp,
        TRY_CAST(FG  AS INT)                                                   AS fg,
        TRY_CAST(FGA AS INT)                                                   AS fga,

        -- percentages like .491 → 0.491
        TRY_CAST(
            CASE WHEN LEFT(CAST("FG%" AS VARCHAR),1)='.'
                 THEN '0'||CAST("FG%" AS VARCHAR)
                 ELSE CAST("FG%" AS VARCHAR)
            END AS DOUBLE
        ) AS fg_pct,

        TRY_CAST("3P"  AS INT)                                                 AS three_p,
        TRY_CAST("3PA" AS INT)                                                 AS three_pa,
        TRY_CAST(
            CASE WHEN LEFT(CAST("3P%" AS VARCHAR),1)='.'
                 THEN '0'||CAST("3P%" AS VARCHAR)
                 ELSE CAST("3P%" AS VARCHAR)
            END AS DOUBLE
        ) AS three_pct,

        TRY_CAST("2P"  AS INT)                                                 AS two_p,
        TRY_CAST("2PA" AS INT)                                                 AS two_pa,
        TRY_CAST(
            CASE WHEN LEFT(CAST("2P%" AS VARCHAR),1)='.'
                 THEN '0'||CAST("2P%" AS VARCHAR)
                 ELSE CAST("2P%" AS VARCHAR)
            END AS DOUBLE
        ) AS two_pct,

        TRY_CAST(FT  AS INT)                                                   AS ft,
        TRY_CAST(FTA AS INT)                                                   AS fta,
        TRY_CAST(
            CASE WHEN LEFT(CAST("FT%" AS VARCHAR),1)='.'
                 THEN '0'||CAST("FT%" AS VARCHAR)
                 ELSE CAST("FT%" AS VARCHAR)
            END AS DOUBLE
        ) AS ft_pct,

        TRY_CAST(ORB AS INT)                                                   AS orb,
        TRY_CAST(DRB AS INT)                                                   AS drb,
        TRY_CAST(TRB AS INT)                                                   AS trb,
        TRY_CAST(AST AS INT)                                                   AS ast,
        TRY_CAST(STL AS INT)                                                   AS stl,
        TRY_CAST(BLK AS INT)                                                   AS blk,
        TRY_CAST(TOV AS INT)                                                   AS tov,
        TRY_CAST(PF  AS INT)                                                   AS pf,
        TRY_CAST(PTS AS INT)                                                   AS pts
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
    build_team_stats_parquet(args.season)
