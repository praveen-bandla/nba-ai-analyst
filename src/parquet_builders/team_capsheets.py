# src/parquet_builders/team_capsheets.py
from pathlib import Path
import duckdb
from config.settings import DATASETS, PARQUET_FOLDERS

def build_team_capsheets_parquet() -> Path:
    """
    Read raw team_capsheets.csv and write a single Parquet:
    data/parquet/team_capsheets/team_capsheets.parquet

    - Skips the first "Salary,Salary,..." header row
    - Uses the real header row (Rk,Team,2025-26,...)
    - Strips $ and commas; blanks -> NULL via TRY_CAST
    - Renames season columns to cap_YYYY_YY (no hyphens)
    """
    src_csv = Path(DATASETS["team_capsheets"])
    out_dir = Path(PARQUET_FOLDERS["team_capsheets"])
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "team_capsheets.parquet"

    con = duckdb.connect()  # in-memory for transform -> parquet
    con.execute(f"""
    COPY (
      SELECT
        TRY_CAST(Rk AS INT)                                                         AS rk,
        Team                                                                        AS team,

        TRY_CAST(REGEXP_REPLACE("2025-26",'[\\$,]', '') AS BIGINT)                  AS cap_2025_26,
        TRY_CAST(REGEXP_REPLACE("2026-27",'[\\$,]', '') AS BIGINT)                  AS cap_2026_27,
        TRY_CAST(REGEXP_REPLACE("2027-28",'[\\$,]', '') AS BIGINT)                  AS cap_2027_28,
        TRY_CAST(REGEXP_REPLACE("2028-29",'[\\$,]', '') AS BIGINT)                  AS cap_2028_29,
        TRY_CAST(REGEXP_REPLACE("2029-30",'[\\$,]', '') AS BIGINT)                  AS cap_2029_30,
        TRY_CAST(REGEXP_REPLACE("2030-31",'[\\$,]', '') AS BIGINT)                  AS cap_2030_31

      FROM read_csv_auto(
        '{src_csv.as_posix()}',
        header=true,
        skip=1              -- <-- skip the bogus first header row
      )
    )
    TO '{out_path.as_posix()}' (FORMAT PARQUET);
    """)
    con.close()
    print(f"wrote {out_path}")
    return out_path

if __name__ == "__main__":
    build_team_capsheets_parquet()
