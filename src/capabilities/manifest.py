# Everything the LLM planners may need lives here.
MANIFEST = {
  "tables": {
    "player_stats": {
      "path": "data/parquet/player_stats.parquet",
      "columns": [
        "player_id","player","team","season","age","g","gs","mp",
        "fg","fga","fg_pct","three_p","three_pa","three_pct",
        "two_p","two_pa","two_pct","efg_pct",
        "ft","fta","ft_pct","orb","drb","trb","ast","stl","blk","tov","pf","pts","trip_dbl","awards"
      ]
    },
    "player_contracts": {
      "path": "data/parquet/player_contracts.parquet",
      "columns": [
        "id","name","team",
        "salary_2025_26","salary_2026_27","salary_2027_28","salary_2028_29","salary_2029_30","salary_2030_31",
        "total_guaranteed","player_id","note"
      ]
    },
    "team_stats": {
      "path": "data/parquet/team_stats.parquet",
      "columns": [
        "team","season","g","mp","fg","fga","fg_pct","three_p","three_pa","three_pct",
        "two_p","two_pa","two_pct","ft","fta","ft_pct","orb","drb","trb","ast","stl","blk","tov","pf","pts"
      ]
    },
    "team_capsheets": {
      "path": "data/parquet/team_capsheets.parquet",
      "columns": [
        "team","cap_2025_26","cap_2026_27","cap_2027_28","cap_2028_29","cap_2029_30","cap_2030_31"
      ]
    },
    "team_picks": {
      "path": "data/parquet/team_picks.parquet",
      "columns": [
        "team","pick_year","pick_round","details"
      ]
    }
  },

  # Metric & wording glossary → canonical column names
  "glossary": {
    "metrics": {
      "3pt%": "three_pct", "3p%": "three_pct", "three point %": "three_pct",
      "three point percentage": "three_pct", "three percentage": "three_pct",
      "threes made": "three_p", "3pm": "three_p", "threes attempted": "three_pa", "3pa": "three_pa",
      "ft%": "ft_pct", "free throw %": "ft_pct",
      "points": "pts", "pts": "pts", "assists": "ast", "rebounds": "trb",
      "blocks": "blk", "steals": "stl",
      "fg%": "fg_pct", "efg%": "efg_pct",
      "salary": "salary"  # season-specific column resolved later
    },
    "verbs": {
      "highest": "rank_desc", "best": "rank_desc", "leader": "rank_desc",
      "average": "mean", "avg": "mean", "mean": "mean",
      "compare": "compare", "more": "compare"
    }
  },

  # Team aliases: canonical → alternatives (lowercased for matching)
  "team_aliases": {
    "cleveland cavaliers": ["cavs", "cleveland", "cle"],
    "golden state warriors": ["warriors", "gsw", "dubs"],
    "phoenix suns": ["suns", "phx"],
    "boston celtics": ["celtics", "bos"],
    "new york knicks": ["knicks", "nyk"]
  },

  # Player aliases: canonical → nicknames/short forms (lowercased)
  "player_aliases": {
    "stephen curry": ["steph", "steph curry", "chef curry", "curry"],
    "kevin durant": ["kd", "durant", "easy money"],
    "giannis antetokounmpo": ["giannis"],
    "luka doncic": ["luka"],
    "jimmy butler": ["jimmy", "himmy"],
    "anthony davis": ["ad", "davis"]
    # add more over time; LLM can still propose canonical names, you normalize here
  },

  # Season phrases (no separate seasons.py)
  "seasons": {
    "phrase_map": {
      "last year": "2024-25",
      "this year": "2025-26",
      "next year": "2026-27"
    },
    # bare "2026" → "2026-27"
    "defaults": {
      "salary": "2025-26",
      "stats": "2024-25"
    }
  },

  # Lightweight dataset docs the planner can reference
  "dataset_docs": {
    "player_stats": "Per-player season totals and percentages. One row per player per season.",
    "player_contracts": "Per-player salary columns by season (salary_YYYY_YY).",
    "team_stats": "Per-team season totals and percentages (league average = mean across teams).",
    "team_capsheets": "Per-team cap totals by season (cap_YYYY_YY).",
    "team_picks": "Textual future pick obligations and swaps."
  }
}
