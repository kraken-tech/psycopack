from typing import Any

from . import _logging, _psycopg


class LoggedCursor:
    """
    A wrapper around Psycopg cursors to execute SQL statement with logging.
    """

    def __init__(self, *, cur: _psycopg.Cursor) -> None:
        self.cur = cur

    def execute(self, sql: str) -> None:
        _logging.logger.debug(sql)
        try:
            self.cur.execute(sql)
        except Exception:
            _logging.logger.exception(f"Failed to execute statement: {sql}")
            raise

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.cur.fetchall()

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.cur.fetchone()
