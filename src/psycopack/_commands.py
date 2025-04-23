import hashlib
from contextlib import contextmanager
from textwrap import dedent
from typing import Iterator

from . import _cur, _introspect
from . import _psycopg as psycopg


class Command:
    def __init__(self, *, conn: psycopg.Connection, cur: _cur.LoggedCursor) -> None:
        self.conn = conn
        self.cur = cur
        self.introspector = _introspect.Introspector(conn=self.conn, cur=self.cur)

    def drop_constraint(self, *, table: str, constraint: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                ALTER TABLE {table}
                DROP CONSTRAINT {constraint};
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                constraint=psycopg.sql.Identifier(constraint),
            )
            .as_string(self.conn)
        )

    def drop_table_if_exists(self, *, table: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("DROP TABLE IF EXISTS {table}")
            .format(table=psycopg.sql.Identifier(table))
            .as_string(self.conn)
        )

    def create_copy_table(self, *, base_table: str, copy_table: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE TABLE {copy_table}
                (LIKE {table} INCLUDING DEFAULTS);
                """)
            )
            .format(
                table=psycopg.sql.Identifier(base_table),
                copy_table=psycopg.sql.Identifier(copy_table),
            )
            .as_string(self.conn)
        )

    def drop_sequence_if_exists(self, *, seq: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("DROP SEQUENCE IF EXISTS {seq};")
            .format(seq=psycopg.sql.Identifier(seq))
            .as_string(self.conn)
        )

    def create_sequence(self, *, seq: str, bigint: bool) -> None:
        if bigint:
            sql = "CREATE SEQUENCE {seq} AS BIGINT;"
        else:
            sql = "CREATE SEQUENCE {seq};"

        self.cur.execute(
            psycopg.sql.SQL(sql)
            .format(seq=psycopg.sql.Identifier(seq))
            .as_string(self.conn)
        )

    def set_table_id_seq(self, *, table: str, seq: str, pk_column: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                ALTER TABLE {table}
                ALTER COLUMN {pk_column}
                SET DEFAULT nextval('{seq}');
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                pk_column=psycopg.sql.Identifier(pk_column),
                seq=psycopg.sql.Identifier(seq),
            )
            .as_string(self.conn)
        )

    def add_pk(self, *, table: str, pk_column: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("ALTER TABLE {table} ADD PRIMARY KEY ({pk_column});")
            .format(
                table=psycopg.sql.Identifier(table),
                pk_column=psycopg.sql.Identifier(pk_column),
            )
            .as_string(self.conn)
        )

    def create_copy_function(
        self,
        *,
        function: str,
        table_from: str,
        table_to: str,
        columns: list[str],
        pk_column: str,
    ) -> None:
        # Note: assumes "id" to be the primary key.
        # TODO: generalise so other PK types can work.
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE OR REPLACE FUNCTION {function}(INTEGER, INTEGER)
                RETURNS VOID AS $$

                  INSERT INTO {table_to}
                  OVERRIDING SYSTEM VALUE
                  SELECT {columns}
                  FROM {table_from}
                  WHERE {pk_column} BETWEEN $1 AND $2
                  ON CONFLICT DO NOTHING

                $$ LANGUAGE SQL SECURITY DEFINER;
                """)
            )
            .format(
                function=psycopg.sql.Identifier(function),
                table_from=psycopg.sql.Identifier(table_from),
                table_to=psycopg.sql.Identifier(table_to),
                columns=psycopg.sql.SQL(",").join(
                    [psycopg.sql.Identifier(c) for c in columns]
                ),
                pk_column=psycopg.sql.Identifier(pk_column),
            )
            .as_string(self.conn)
        )

    def drop_trigger_if_exists(self, *, table: str, trigger: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("DROP TRIGGER IF EXISTS {trigger} ON {table}")
            .format(
                trigger=psycopg.sql.Identifier(trigger),
                table=psycopg.sql.Identifier(table),
            )
            .as_string(self.conn)
        )

    def create_copy_trigger(
        self,
        trigger_name: str,
        function: str,
        table_from: str,
        table_to: str,
        pk_column: str,
    ) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE OR REPLACE FUNCTION {trigger_name}()
                RETURNS TRIGGER AS
                $$
                BEGIN
                  IF ( TG_OP = 'INSERT') THEN
                    PERFORM {function}(NEW.{pk_column}, NEW.{pk_column});
                    RETURN NEW;
                  ELSIF ( TG_OP = 'UPDATE') THEN
                    DELETE FROM {table_to} WHERE {pk_column} = OLD.{pk_column};
                    PERFORM {function}(NEW.{pk_column}, NEW.{pk_column});
                    RETURN NEW;
                  ELSIF ( TG_OP = 'DELETE') THEN
                    DELETE FROM {table_to} WHERE {pk_column} = OLD.{pk_column};
                    RETURN OLD;
                  END IF;
                END;
                $$ LANGUAGE PLPGSQL SECURITY DEFINER;

                CREATE TRIGGER {trigger_name}
                AFTER INSERT OR UPDATE OR DELETE ON {table_from}
                FOR EACH ROW EXECUTE PROCEDURE {trigger_name}();
                """)
            )
            .format(
                trigger_name=psycopg.sql.Identifier(trigger_name),
                function=psycopg.sql.Identifier(function),
                table_from=psycopg.sql.Identifier(table_from),
                table_to=psycopg.sql.Identifier(table_to),
                pk_column=psycopg.sql.Identifier(pk_column),
            )
            .as_string(self.conn)
        )

    def create_backfill_log(self, *, table: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE TABLE {table} (
                  id SERIAL PRIMARY KEY,
                  batch_start INT,
                  batch_end INT,
                  finished BOOLEAN
                );
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
            )
            .as_string(self.conn)
        )
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE INDEX {log_index}
                ON {table} ("finished")
                WHERE finished IS FALSE;
                """)
            )
            .format(
                log_index=psycopg.sql.Identifier(f"{table}_idx"),
                table=psycopg.sql.Identifier(table),
            )
            .as_string(self.conn)
        )

    @contextmanager
    def session_lock(self, *, name: str) -> Iterator[None]:
        # Based on:
        # https://github.com/Opus10/django-pglock/blob/bf7422d3a74eed8196e13f6b28b72fb0623560e5/pglock/core.py#L137-L139
        key = int.from_bytes(
            hashlib.sha256(name.encode("utf-8")).digest()[:8], "little", signed=True
        )
        self.cur.execute(f"SELECT pg_advisory_lock({key});")
        yield
        self.cur.execute(f"SELECT pg_advisory_unlock({key});")

    def populate_backfill_log(
        self,
        table: str,
        batch_size: int,
        min_pk: int,
        max_pk: int,
    ) -> None:
        batches = (
            (batch_start, min(batch_start + batch_size - 1, max_pk), False)
            for batch_start in range(min_pk, max_pk + 1, batch_size)
        )
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                INSERT INTO {table} (batch_start, batch_end, finished)
                VALUES {batches};
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                batches=psycopg.sql.SQL(", ").join(
                    map(psycopg.sql.SQL, [str(batch) for batch in batches])
                ),
            )
            .as_string(self.conn)
        )

    def create_unique_constraint_using_idx(
        self,
        table: str,
        constraint: str,
        index: str,
        is_deferrable: bool,
        is_deferred: bool,
    ) -> None:
        add_constraint_sql = dedent("""
            ALTER TABLE {table}
            ADD CONSTRAINT {constraint}
            UNIQUE USING INDEX {index}
        """)
        if is_deferrable:
            add_constraint_sql += " DEFERRABLE"
        else:
            add_constraint_sql += " NOT DEFERRABLE"

        if is_deferred:
            add_constraint_sql += " INITIALLY DEFERRED"
        else:
            add_constraint_sql += " INITIALLY IMMEDIATE"

        self.cur.execute(
            psycopg.sql.SQL(add_constraint_sql)
            .format(
                table=psycopg.sql.Identifier(table),
                constraint=psycopg.sql.Identifier(constraint),
                index=psycopg.sql.Identifier(index),
            )
            .as_string(self.conn)
        )

    def create_not_valid_constraint_from_def(
        self, *, table: str, constraint: str, definition: str, is_validated: bool
    ) -> None:
        add_constraint_sql = dedent("""
            ALTER TABLE {table}
            ADD CONSTRAINT {constraint}
            {definition}
        """)
        if is_validated:
            # If the definition is for a valid constraint, alter it to be not
            # valid manually so that it can be created ONLINE.
            add_constraint_sql += " NOT VALID"
        self.cur.execute(
            psycopg.sql.SQL(add_constraint_sql)
            .format(
                table=psycopg.sql.Identifier(table),
                constraint=psycopg.sql.Identifier(constraint),
                definition=psycopg.sql.SQL(definition),
            )
            .as_string(self.conn)
        )

    def validate_constraint(self, *, table: str, constraint: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                ALTER TABLE {table}
                VALIDATE CONSTRAINT {constraint}
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                constraint=psycopg.sql.Identifier(constraint),
            )
            .as_string(self.conn)
        )

    def drop_function_if_exists(self, *, function: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("DROP FUNCTION IF EXISTS {function};")
            .format(function=psycopg.sql.Identifier(function))
            .as_string(self.conn)
        )

    def rename_table(self, *, table_from: str, table_to: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("ALTER TABLE {table_from} RENAME TO {table_to};")
            .format(
                table_from=psycopg.sql.Identifier(table_from),
                table_to=psycopg.sql.Identifier(table_to),
            )
            .as_string(self.conn)
        )

    def rename_index(self, *, idx_from: str, idx_to: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("ALTER INDEX {idx_from} RENAME TO {idx_to};")
            .format(
                idx_from=psycopg.sql.Identifier(idx_from),
                idx_to=psycopg.sql.Identifier(idx_to),
            )
            .as_string(self.conn)
        )

    def rename_sequence(self, *, seq_from: str, seq_to: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("ALTER SEQUENCE {seq_from} RENAME TO {seq_to};")
            .format(
                seq_from=psycopg.sql.Identifier(seq_from),
                seq_to=psycopg.sql.Identifier(seq_to),
            )
            .as_string(self.conn)
        )

    def rename_constraint(self, *, table: str, cons_from: str, cons_to: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                "ALTER TABLE {table} RENAME CONSTRAINT {cons_from} TO {cons_to};"
            )
            .format(
                table=psycopg.sql.Identifier(table),
                cons_from=psycopg.sql.Identifier(cons_from),
                cons_to=psycopg.sql.Identifier(cons_to),
            )
            .as_string(self.conn)
        )

    def create_constraint(self, *, table: str, name: str, definition: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                ALTER TABLE {table}
                ADD CONSTRAINT {constraint}
                {definition}
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                constraint=psycopg.sql.Identifier(name),
                definition=psycopg.sql.SQL(definition),
            )
            .as_string(self.conn)
        )

    def set_generated_identity(
        self, *, table: str, always: bool, pk_column: str
    ) -> None:
        identity_type = "ALWAYS" if always else "BY DEFAULT"
        self.cur.execute(
            sql=psycopg.sql.SQL(
                dedent("""
                ALTER TABLE {table}
                ALTER COLUMN {pk_column}
                ADD GENERATED {identity_type} AS IDENTITY;
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                pk_column=psycopg.sql.Identifier(pk_column),
                identity_type=psycopg.sql.SQL(identity_type),
            )
            .as_string(self.conn)
        )

    def convert_pk_to_bigint(self, *, table: str, seq: str, pk_column: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                ALTER TABLE {table}
                ALTER COLUMN {pk_column}
                SET DATA TYPE BIGINT;
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                pk_column=psycopg.sql.Identifier(pk_column),
            )
            .as_string(self.conn)
        )

    def swap_pk_sequence_name(self, *, first_table: str, second_table: str) -> None:
        first_seq = self.introspector.get_pk_sequence_name(table=first_table)
        second_seq = self.introspector.get_pk_sequence_name(table=second_table)
        temp_seq = f"{first_seq}_temp"

        with self.db_transaction():
            self.rename_sequence(seq_from=first_seq, seq_to=temp_seq)
            self.rename_sequence(seq_from=second_seq, seq_to=first_seq)
            self.rename_sequence(seq_from=temp_seq, seq_to=second_seq)

    @contextmanager
    def db_transaction(self) -> Iterator[None]:
        self.cur.execute("BEGIN;")
        yield
        self.cur.execute("COMMIT;")
