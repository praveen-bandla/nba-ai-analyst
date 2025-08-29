# src/parquet_builders/player_contracts.py
from pathlib import Path
import duckdb
from config.settings import DATASETS, PARQUET_FOLDERS

def build_player_contracts_parquet() -> Path:
    src_csv = Path(DATASETS["player_contracts"])
    out_dir = Path(PARQUET_FOLDERS["player_contracts"])
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "player_contracts.parquet"

    con = duckdb.connect()  # in-memory for the transform → parquet write
    # Strip $ and commas; tolerate blanks with TRY_CAST → NULLs
    con.execute(f"""
    COPY (
      SELECT
        CAST(id AS INT)                                                               AS id,
        name                                                                          AS name,
        team                                                                          AS team,

        TRY_CAST(REGEXP_REPLACE(Salary,     '[\\$,]', '') AS BIGINT)                  AS salary_2025_26,
        TRY_CAST(REGEXP_REPLACE("Salary.1", '[\\$,]', '') AS BIGINT)                  AS salary_2026_27,
        TRY_CAST(REGEXP_REPLACE("Salary.2", '[\\$,]', '') AS BIGINT)                  AS salary_2027_28,
        TRY_CAST(REGEXP_REPLACE("Salary.3", '[\\$,]', '') AS BIGINT)                  AS salary_2028_29,
        TRY_CAST(REGEXP_REPLACE("Salary.4", '[\\$,]', '') AS BIGINT)                  AS salary_2029_30,
        TRY_CAST(REGEXP_REPLACE("Salary.5", '[\\$,]', '') AS BIGINT)                  AS salary_2030_31,

        TRY_CAST(REGEXP_REPLACE("Unnamed: 9",'[\\$,]', '') AS BIGINT)                 AS total_guaranteed,
        "-additional"                                                                 AS player_id,
        Note                                                                          AS note
      FROM read_csv_auto('{src_csv.as_posix()}', header=true)
    )
    TO '{out_path.as_posix()}' (FORMAT PARQUET);
    """)
    con.close()
    print(f"wrote {out_path}")
    return out_path

if __name__ == "__main__":
    build_player_contracts_parquet()
