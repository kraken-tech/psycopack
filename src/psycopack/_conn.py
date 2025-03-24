from typing import cast

from . import _psycopg as psycopg


def get_db_connection(
    dsn: str,
    autocommit: bool = True,
) -> psycopg.Connection:
    if psycopg.PSYCOPG_3:
        return cast(
            psycopg.Connection,
            psycopg.connect(dsn, autocommit=autocommit),
        )
    else:  # pragma: no cover
        # psycopg version 2. We run coverage for psycopg 3 only.
        conn = psycopg.connect(dsn)
        conn.autocommit = True
        return conn
