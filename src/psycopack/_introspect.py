import dataclasses
from textwrap import dedent

from . import _const, _cur
from . import _psycopg as psycopg


@dataclasses.dataclass
class Index:
    name: str
    definition: str
    is_primary: bool
    is_exclusion: bool


@dataclasses.dataclass
class Constraint:
    name: str
    definition: str
    is_deferrable: bool
    is_deferred: bool
    is_validated: bool


@dataclasses.dataclass
class ReferringForeignKey:
    name: str
    definition: str
    is_validated: bool
    referring_table: str


@dataclasses.dataclass
class Trigger:
    name: str
    is_internal: bool
    is_psycopack_trigger: bool


@dataclasses.dataclass
class PrimaryKey:
    columns: list[str]
    data_types: list[str]
    identity_type: str


class Introspector:
    def __init__(self, *, conn: psycopg.Connection, cur: _cur.LoggedCursor) -> None:
        self.conn = conn
        self.cur = cur

    def get_table_oid(self, *, table: str) -> int | None:
        self.cur.execute(
            psycopg.sql.SQL("SELECT oid FROM pg_class WHERE relname = {table};")
            .format(table=psycopg.sql.Literal(table))
            .as_string(self.conn)
        )
        result = self.cur.fetchone()
        if result is None:
            return None

        oid = result[0]
        assert isinstance(oid, int)
        return oid

    def get_table_columns(self, *, table: str) -> list[str]:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  column_name
                FROM
                  information_schema.columns
                WHERE
                  table_name = {table}
                ORDER BY
                  ordinal_position;
                """)
            )
            .format(table=psycopg.sql.Literal(table))
            .as_string(self.conn)
        )
        return [r[0] for r in self.cur.fetchall()]

    def get_min_and_max_pk(self, *, table: str, pk_column: str) -> tuple[int, int]:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  MIN({pk_column}) AS min_pk,
                  MAX({pk_column}) AS max_pk
                FROM {table};
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                pk_column=psycopg.sql.Identifier(pk_column),
            )
            .as_string(self.conn)
        )
        result = self.cur.fetchone()
        assert result is not None
        min_pk, max_pk = result
        assert isinstance(min_pk, int)
        assert isinstance(max_pk, int)
        return min_pk, max_pk

    def get_index_def(self, *, table: str) -> list[Index]:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  pg_indexes.indexname,
                  pg_indexes.indexdef,
                  pg_index.indisprimary,
                  pg_index.indisexclusion
                FROM
                  pg_indexes
                INNER JOIN
                  pg_class
                  ON pg_class.relname = pg_indexes.indexname
                INNER JOIN
                  pg_index
                  ON pg_index.indexrelid = pg_class.oid
                WHERE
                  pg_indexes.tablename = {table}
                ORDER BY
                  pg_indexes.indexname,
                  pg_indexes.indexdef;
                """)
            )
            .format(table=psycopg.sql.Literal(table))
            .as_string(self.conn)
        )
        results = self.cur.fetchall()
        assert results is not None
        return [
            Index(
                name=name,
                definition=definition,
                is_primary=is_primary,
                is_exclusion=is_exclusion,
            )
            for name, definition, is_primary, is_exclusion in results
        ]

    def get_constraints(self, *, table: str, types: list[str]) -> list[Constraint]:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  conname as constraint_name,
                  pg_get_constraintdef(oid) AS definition,
                  condeferrable as is_deferrable,
                  condeferred as is_deferred,
                  convalidated as is_validated
                FROM
                  pg_constraint
                WHERE
                  conrelid = {table}::regclass
                  AND contype IN ({types})
                ORDER BY
                  constraint_name,
                  definition;
                """)
            )
            .format(
                table=psycopg.sql.Literal(table),
                types=psycopg.sql.SQL(", ").join(map(psycopg.sql.Literal, types)),
            )
            .as_string(self.conn)
        )
        results = self.cur.fetchall()
        assert results is not None
        return [
            Constraint(
                name=name,
                definition=definition,
                is_deferrable=is_deferrable,
                is_deferred=is_deferred,
                is_validated=is_validated,
            )
            for name, definition, is_deferrable, is_deferred, is_validated in results
        ]

    def get_referring_fks(self, *, table: str) -> list[ReferringForeignKey]:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  cons.conname AS constraint_name,
                  pg_get_constraintdef(cons.oid) AS definition,
                  cons.convalidated AS is_validated,
                  class.relname AS referring_table
                FROM
                  pg_constraint AS cons
                INNER JOIN
                  pg_class AS class
                  ON (cons.conrelid = class.oid)
                WHERE
                  confrelid = {table}::regclass
                  AND contype = 'f'
                ORDER BY
                  constraint_name,
                  definition;
                """)
            )
            .format(table=psycopg.sql.Literal(table))
            .as_string(self.conn)
        )
        results = self.cur.fetchall()
        assert results is not None
        return [
            ReferringForeignKey(
                name=name,
                definition=definition,
                is_validated=is_validated,
                referring_table=referring_table,
            )
            for name, definition, is_validated, referring_table in results
        ]

    def table_is_empty(self, *, table: str) -> int:
        self.cur.execute(
            psycopg.sql.SQL("SELECT NOT EXISTS (SELECT 1 FROM {table} LIMIT 1);")
            .format(table=psycopg.sql.Identifier(table))
            .as_string(self.conn)
        )
        result = self.cur.fetchone()
        assert result is not None
        return bool(result[0])

    def trigger_exists(self, *, trigger: str) -> bool:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  1
                FROM
                  pg_trigger
                WHERE
                  tgname = {trigger};
                """)
            )
            .format(trigger=psycopg.sql.Literal(trigger))
            .as_string(self.conn)
        )
        return bool(self.cur.fetchone())

    def is_inherited_table(self, *, table: str) -> bool:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  1
                FROM
                  pg_inherits
                WHERE
                  inhrelid = {table}::regclass;
                """)
            )
            .format(table=psycopg.sql.Literal(table))
            .as_string(self.conn)
        )
        return bool(self.cur.fetchone())

    def get_triggers(self, *, table: str) -> list[Trigger]:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  tgname AS name,
                  tgisinternal AS is_internal,
                  (
                    tgname LIKE '%' || {name_prefix} || '%'
                    OR tgname LIKE '%' || {repacked_name_prefix} || '%'
                  ) AS is_psycopack_trigger
                FROM
                  pg_trigger
                WHERE
                  tgrelid = {table}::regclass
                """)
            )
            .format(
                table=psycopg.sql.Literal(table),
                name_prefix=psycopg.sql.Literal(_const.NAME_PREFIX),
                repacked_name_prefix=psycopg.sql.Literal(_const.REPACKED_NAME_PREFIX),
            )
            .as_string(self.conn)
        )
        results = self.cur.fetchall()
        return [
            Trigger(
                name=name,
                is_internal=is_internal,
                is_psycopack_trigger=is_psycopack_trigger,
            )
            for name, is_internal, is_psycopack_trigger in results
        ]

    def get_primary_key_info(self, *, table: str) -> PrimaryKey | None:
        # Based on:
        # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  attr.attname AS column_name,
                  format_type(attr.atttypid, attr.atttypmod) AS data_type,
                  attr.attidentity AS identity_type
                FROM
                  pg_index idx
                JOIN
                  pg_attribute attr
                  ON attr.attrelid = idx.indrelid AND attr.attnum = ANY(idx.indkey)
                WHERE
                  idx.indrelid = {table}::regclass
                  AND idx.indisprimary;
                """)
            )
            .format(table=psycopg.sql.Literal(table))
            .as_string(self.conn)
        )
        if not (results := self.cur.fetchall()):
            return None
        return PrimaryKey(
            columns=[col for col, _, _ in results],
            data_types=[dt for _, dt, _ in results],
            identity_type=next(identity for _, _, identity in results),
        )

    def get_pk_sequence_name(self, *, table: str) -> str:
        pk_info = self.get_primary_key_info(table=table)
        assert pk_info is not None
        assert len(pk_info.columns) == 1

        if pk_info.identity_type:
            self.cur.execute(
                psycopg.sql.SQL("SELECT pg_get_serial_sequence({table}, {column});")
                .format(
                    table=psycopg.sql.Literal(table),
                    column=psycopg.sql.Literal(pk_info.columns[0]),
                )
                .as_string(self.conn)
            )
            result = self.cur.fetchone()
            assert result is not None
            # The result is like `schema.name`.
            seq = result[0].split(".")[1]
            assert isinstance(seq, str)
            return seq
        else:
            # For non-identity primary keys such as serial and manually-defined
            # sequence fields, the "pg_get_serial_sequence" does not work.
            # Also, this query does not work for identity tables so this
            # function can't be further generalised.
            self.cur.execute(
                psycopg.sql.SQL(
                    dedent("""
                    SELECT
                      (
                        SELECT
                          pg_catalog.pg_get_expr(attdef.adbin, attdef.adrelid, true)
                        FROM
                          pg_catalog.pg_attrdef attdef
                        WHERE
                          attdef.adrelid = att.attrelid
                          AND attdef.adnum = att.attnum
                          AND att.atthasdef
                      ) AS seq_def
                    FROM
                      pg_catalog.pg_attribute att
                    WHERE
                      att.attrelid = {table}::regclass
                      AND att.attname = {column}
                    ORDER BY att.attnum;
                    """)
                )
                .format(
                    table=psycopg.sql.Literal(table),
                    column=psycopg.sql.Literal(pk_info.columns[0]),
                )
                .as_string(self.conn)
            )
            result = self.cur.fetchone()
            assert result is not None
            seq_def = result[0]
            assert isinstance(seq_def, str)
            # The seq_def variable looks something like:
            #  nextval('psycopack_2999727_id_seq'::regclass)
            # So we need to parse the sequence name out.
            start: int = seq_def.find("'") + 1
            end: int = seq_def.find("'", start)
            return seq_def[start:end]
