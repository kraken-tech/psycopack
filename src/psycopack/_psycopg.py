"""
Support package to make sure psycopack works with either psycopg2 or psycopg
"""

PSYCOPG_3 = False

try:
    from psycopg import Connection, Cursor, connect, errors, sql

    PSYCOPG_3 = True
except ImportError:  # pragma: no cover
    try:
        from psycopg2 import (  # type: ignore[no-redef, assignment] # noqa: F401
            connect,
            errors,
            sql,
        )
        from psycopg2.extensions import (  # type: ignore[assignment]
            connection as Connection,
        )
        from psycopg2.extensions import cursor as Cursor  # type: ignore[assignment]
    except ImportError as exc:
        raise ImportError("Neither psycopg2 nor psycopg (3) is installed.") from exc


__all__ = (
    "Connection",
    "Cursor",
    "PSYCOPG_3",
    "connect",
    "sql",
    "errors",
)
