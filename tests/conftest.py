import os
from typing import Generator

import pytest

from psycopack import _conn, _cur
from psycopack import _psycopg as psycopg


DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
)
DATABASE_NAME = os.getenv("DATABASE_NAME", "test_psycopack")


@pytest.fixture
def connection() -> Generator[psycopg.Connection, None, None]:
    with _conn.get_db_connection(DATABASE_URL) as conn:
        with _cur.get_cursor(conn) as cur:
            cur.execute(f'DROP DATABASE IF EXISTS "{DATABASE_NAME}" WITH (FORCE);')
            cur.execute(f'CREATE DATABASE "{DATABASE_NAME}"')

        # Return (a new) connection. You can't directly change the db for an
        # existing connection. Once a connection is created for a db, it is
        # tied to that db.
        new_db_url = "/".join(DATABASE_URL.split("/")[:-1])
        new_db_url += f"/{DATABASE_NAME}"
        with _conn.get_db_connection(new_db_url) as new_conn:
            yield new_conn

        with _cur.get_cursor(conn) as cur:
            cur.execute(f'DROP DATABASE IF EXISTS "{DATABASE_NAME}" WITH (FORCE)')
