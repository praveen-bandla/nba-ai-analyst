# src/tools/base/base_sql_tool.py  (or your path)
from __future__ import annotations
from typing import Any, Dict, Optional, Sequence, Tuple, Type

import duckdb
from pydantic import BaseModel, PrivateAttr
from langchain.tools import BaseTool

class BaseSQLTool(BaseTool):
    """
    LangChain Tool base for deterministic SQL/Parquet compute via DuckDB.
    Subclasses must implement:
      - build_sql_and_params(**kwargs) -> (sql: str, params: Sequence[Any])
      - name: str
      - description: str
      - args_schema: type[pydantic.BaseModel]
    """

    # Public (pydantic) fields
    name: str
    description: str
    parquet_path: str
    db_path: Optional[str] = None
    args_schema: Type[BaseModel]  # set on subclass

    # Private attrs (not validated/serialized by pydantic)
    _db: duckdb.DuckDBPyConnection = PrivateAttr()

    def __init__(self, *, name: str, description: str, parquet_path: str, db_path: Optional[str] = None, **kwargs: Any):
        super().__init__(name=name, description=description, parquet_path=parquet_path, db_path=db_path, **kwargs)
        self._db = duckdb.connect(db_path or ":memory:")

    # Subclass hook
    def build_sql_and_params(self, **kwargs) -> Tuple[str, Sequence[Any]]:
        raise NotImplementedError

    # LangChain Tool API
    def _run(self, tool_input: Dict[str, Any], **_) -> Any:
        sql, params = self.build_sql_and_params(**tool_input)
        res = self._db.execute(sql, params).fetchall()
        desc = getattr(self._db, "description", None)
        cols = [d[0] for d in desc] if desc else []
        if len(cols) == 1:
            return res[0][0] if res else None
        return [dict(zip(cols, row)) for row in res]

    async def _arun(self, tool_input: Dict[str, Any], **_) -> Any:
        # DuckDB is sync; you can add a threadpool here later if needed
        return self._run(tool_input)
