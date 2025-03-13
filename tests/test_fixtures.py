from psycopack._psycopg import Connection


def test_connection(connection: Connection) -> None:
    """
    Verify that the `connection` fixture provides a valid database connection
    allowing for the creation and deletion of tables.
    """
    with connection.cursor() as cur:
        cur.execute("CREATE TABLE fixture_test (id SERIAL PRIMARY KEY);")
        cur.execute("DROP TABLE fixture_test;")
