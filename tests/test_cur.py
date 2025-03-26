from unittest.mock import patch

import pytest

from psycopack import _cur, _logging, _psycopg


def test_valid_query(connection: _psycopg.Connection) -> None:
    sql = "SELECT 1;"

    with connection.cursor() as cur:
        logged_cursor = _cur.LoggedCursor(cur=cur)

        with patch.object(_logging.logger, "debug") as mock_debug:
            logged_cursor.execute(sql)

        mock_debug.assert_called_once_with(sql)


def test_invalid_query(connection: _psycopg.Connection) -> None:
    sql = "SELECT * FROM table_that_does_not_exist;"

    with connection.cursor() as cur:
        logged_cursor = _cur.LoggedCursor(cur=cur)

        with patch.object(_logging.logger, "debug") as mock_debug:
            with patch.object(_logging.logger, "exception") as mock_exception:
                with pytest.raises(Exception):
                    logged_cursor.execute(sql)

        mock_debug.assert_called_once_with(sql)
        mock_exception.assert_called_once_with(f"Failed to execute statement: {sql}")
