from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import pyodbc

from src.config import settings


@contextmanager
def open_connection() -> Iterator[pyodbc.Connection]:
    settings.validate()
    conn = pyodbc.connect(settings.sql_connection_string, autocommit=False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_batch(
    conn: pyodbc.Connection,
    statements: list[tuple[str, list[Any]]],
) -> None:
    cursor = conn.cursor()
    for sql, params in statements:
        cursor.execute(sql, params)
    cursor.close()


def query_rows(
    conn: pyodbc.Connection,
    sql: str,
    params: list[Any] | None = None,
) -> list[dict[str, Any]]:
    cursor = conn.cursor()
    cursor.execute(sql, params or [])
    columns = [c[0] for c in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    return rows
