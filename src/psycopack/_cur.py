from contextlib import contextmanager
from typing import Any, Iterator

from . import _logging, _psycopg


class LoggedCursor:
    """
    A wrapper around Psycopg cursors to execute SQL statement with logging.
    """

    def __init__(self, *, cur: _psycopg.Cursor) -> None:
        self.cur = cur

    def execute(self, sql: str, /) -> None:
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


@contextmanager
def get_cursor(conn: _psycopg.Connection) -> Iterator[LoggedCursor]:
    with conn.cursor() as cur:
        if not _psycopg.PSYCOPG_3:  # pragma: no cover
            # Psycopg 2 has an unfortunate design decision where calling the
            # cursor in a context manager already starts a transaction by
            # default.
            #
            # Psycopack does not want that because it controls its own
            # transactions.
            #
            # https://github.com/psycopg/psycopg2/issues/941#issuecomment-864025101
            # https://github.com/psycopg/psycopg2/issues/1305#issuecomment-866712961
            cur.execute("ABORT")
        yield LoggedCursor(cur=cur)
