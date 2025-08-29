# src/parquet_builders/team_picks.py
from pathlib import Path
import duckdb
from config.settings import DATASETS, PARQUET_FOLDERS

def build_team_picks_parquet() -> Path:
    """
    Read team_picks.csv and write a single Parquet:
      data/parquet/team_picks/team_picks.parquet

    Keeps data simple:
      - team (TEXT)
      - pick_year (INT)
      - pick_round (TEXT)   # "First" / "Second"
      - details (TEXT)
    """
    src_csv = Path(DATASETS["team_picks"])
    out_dir = Path(PARQUET_FOLDERS["team_picks"])
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "team_picks.parquet"

    con = duckdb.connect()  # in-memory for transform → parquet write
    con.execute(f"""
    COPY (
      SELECT
        TRIM(team)                                  AS team,
        TRY_CAST(year AS INT)                       AS pick_year,
        TRIM("round")                               AS pick_round,
        details                                     AS details
      FROM read_csv_auto('{src_csv.as_posix()}', header=true)
    )
    TO '{out_path.as_posix()}' (FORMAT PARQUET);
    """)
    con.close()
    print(f"wrote {out_path}")
    return out_path

if __name__ == "__main__":
    build_team_picks_parquet()
