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
    is_valid: bool


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
    schema: str
    is_owned_by_user: bool


@dataclasses.dataclass
class ReferredForeignKey:
    name: str
    schema: str
    privileges: list[str]


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


@dataclasses.dataclass
class BackfillBatch:
    id: int
    start: int
    end: int


class Introspector:
    def __init__(
        self, *, conn: psycopg.Connection, cur: _cur.Cursor, schema: str
    ) -> None:
        self.conn = conn
        self.cur = cur
        self.schema = schema

    def get_table_oid(self, *, table: str) -> int | None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  pg_class.oid
                FROM
                  pg_catalog.pg_class
                INNER JOIN
                  pg_catalog.pg_namespace
                  ON (pg_namespace.oid = pg_class.relnamespace)
                WHERE
                  relname = {table}
                  AND pg_namespace.nspname = {schema};
                """)
            )
            .format(
                table=psycopg.sql.Literal(table),
                schema=psycopg.sql.Literal(self.schema),
            )
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
                  AND table_schema = {schema}
                ORDER BY
                  ordinal_position;
                """)
            )
            .format(
                table=psycopg.sql.Literal(table),
                schema=psycopg.sql.Literal(self.schema),
            )
            .as_string(self.conn)
        )
        return [r[0] for r in self.cur.fetchall()]

    def get_min_and_max_pk(
        self, *, table: str, pk_column: str, positive: bool
    ) -> tuple[int, int] | None:
        if positive:
            # positive range
            where_sql = "WHERE {pk_column} >= 0;"
        else:
            # negative range
            where_sql = "WHERE {pk_column} < 0;"
        self.cur.execute(
            psycopg.sql.SQL(
                dedent(
                    """
                    SELECT
                      MIN({pk_column}) AS min_pk,
                      MAX({pk_column}) AS max_pk
                    FROM {schema}.{table}
                    """
                    + where_sql
                )
            )
            .format(
                table=psycopg.sql.Identifier(table),
                schema=psycopg.sql.Identifier(self.schema),
                pk_column=psycopg.sql.Identifier(pk_column),
            )
            .as_string(self.conn)
        )
        result = self.cur.fetchone()
        assert result is not None
        if result == (None, None):
            # table is empty
            return None
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
                  pg_index.indisexclusion,
                  pg_index.indisvalid
                FROM
                  pg_catalog.pg_indexes
                INNER JOIN
                  pg_catalog.pg_class
                  ON pg_class.relname = pg_indexes.indexname
                INNER JOIN
                  pg_catalog.pg_index
                  ON pg_index.indexrelid = pg_class.oid
                WHERE
                  pg_indexes.tablename = {table}
                  AND pg_indexes.schemaname = {schema}
                ORDER BY
                  pg_indexes.indexname,
                  pg_indexes.indexdef;
                """)
            )
            .format(
                table=psycopg.sql.Literal(table),
                schema=psycopg.sql.Literal(self.schema),
            )
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
                is_valid=is_valid,
            )
            for name, definition, is_primary, is_exclusion, is_valid in results
        ]

    def get_constraints(self, *, table: str, types: list[str]) -> list[Constraint]:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  pg_constraint.conname as constraint_name,
                  pg_get_constraintdef(pg_constraint.oid) AS definition,
                  pg_constraint.condeferrable as is_deferrable,
                  pg_constraint.condeferred as is_deferred,
                  pg_constraint.convalidated as is_validated
                FROM
                  pg_catalog.pg_constraint
                INNER JOIN
                  pg_catalog.pg_namespace
                  ON (pg_namespace.oid = pg_constraint.connamespace)
                INNER JOIN
                  pg_catalog.pg_class
                  ON (pg_constraint.conrelid = pg_class.oid)
                WHERE
                  pg_class.relname = {table}
                  AND pg_namespace.nspname = {schema}
                  AND pg_constraint.contype IN ({types})
                ORDER BY
                  constraint_name,
                  definition;
                """)
            )
            .format(
                table=psycopg.sql.Literal(table),
                schema=psycopg.sql.Literal(self.schema),
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
                  pg_constraint.conname AS constraint_name,
                  pg_get_constraintdef(pg_constraint.oid) AS definition,
                  pg_constraint.convalidated AS is_validated,
                  referring_pg_class.relname AS referring_table,
                  pg_namespace.nspname AS schema,
                  (
                    SELECT
                      tableowner = current_user OR tableowner IN (
                        SELECT
                          rolname
                        FROM
                          pg_roles
                        WHERE
                          pg_has_role(current_user, pg_roles.oid, 'MEMBER')
                      )
                    FROM
                      pg_tables
                    WHERE
                      schemaname = {schema}
                      AND tablename = referring_pg_class.relname
                  ) AS is_owned_by_user
                FROM
                  pg_catalog.pg_constraint
                INNER JOIN
                  pg_catalog.pg_class AS referring_pg_class
                  ON (pg_constraint.conrelid = referring_pg_class.oid)
                INNER JOIN
                  pg_catalog.pg_class AS referred_pg_class
                  ON (pg_constraint.confrelid = referred_pg_class.oid)
                INNER JOIN
                  pg_catalog.pg_namespace
                  ON (pg_namespace.oid = referring_pg_class.relnamespace)
                WHERE
                  pg_constraint.confrelid = referred_pg_class.oid
                  AND referred_pg_class.relname = {table}
                  AND contype = 'f'
                ORDER BY
                  constraint_name,
                  definition;
                """)
            )
            .format(
                table=psycopg.sql.Literal(table),
                schema=psycopg.sql.Literal(self.schema),
            )
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
                schema=schema,
                is_owned_by_user=is_owned_by_user,
            )
            for name, definition, is_validated, referring_table, schema, is_owned_by_user in results
        ]

    def get_referred_fks(self, *, table: str, schema: str) -> list[ReferredForeignKey]:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  referred_pg_class.relname AS referred_table,
                  referred_pg_namespace.nspname AS referred_schema,
                  array_agg(
                    table_privileges.privilege_type
                    ORDER BY table_privileges.privilege_type
                  ) AS privileges
                FROM
                  pg_catalog.pg_constraint
                INNER JOIN
                  pg_catalog.pg_class AS referring_pg_class
                  ON (pg_constraint.conrelid = referring_pg_class.oid)
                INNER JOIN
                  pg_catalog.pg_class AS referred_pg_class
                  ON (pg_constraint.confrelid = referred_pg_class.oid)
                INNER JOIN
                  pg_catalog.pg_namespace AS referring_pg_namespace
                  ON (referring_pg_namespace.oid = referring_pg_class.relnamespace)
                INNER JOIN
                  pg_catalog.pg_namespace AS referred_pg_namespace
                  ON (referred_pg_namespace.oid = referred_pg_class.relnamespace)
                LEFT OUTER JOIN
                  information_schema.table_privileges
                  ON (
                    table_privileges.table_name = referred_pg_class.relname
                    AND table_privileges.table_schema = referred_pg_namespace.nspname
                  )
                WHERE
                  pg_constraint.conrelid = referring_pg_class.oid
                  AND referring_pg_class.relname = {table}
                  AND referring_pg_namespace.nspname = {schema}
                  AND pg_constraint.contype = 'f'
                GROUP BY
                  referred_pg_namespace.nspname,
                  referred_pg_class.relname
                ORDER BY
                  referred_pg_namespace.nspname,
                  referred_pg_class.relname;
                """)
            )
            .format(
                table=psycopg.sql.Literal(table),
                schema=psycopg.sql.Literal(schema),
            )
            .as_string(self.conn)
        )
        results = self.cur.fetchall()
        assert results is not None
        return [
            ReferredForeignKey(
                name=name,
                schema=schema,
                privileges=(
                    privileges.strip("{}").split(",")
                    if "NULL" not in privileges
                    else []
                ),
            )
            for name, schema, privileges in results
        ]

    def table_is_empty(self, *, table: str) -> int:
        self.cur.execute(
            psycopg.sql.SQL(
                "SELECT NOT EXISTS (SELECT 1 FROM {schema}.{table} LIMIT 1);"
            )
            .format(
                table=psycopg.sql.Identifier(table),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )
        result = self.cur.fetchone()
        assert result is not None
        return bool(result[0])

    def trigger_exists(self, *, trigger: str) -> bool:
        # Note: Postgres triggers are not schema-qualified. Triggers inherit
        # the schema of their tables.
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
                  pg_catalog.pg_inherits
                INNER JOIN
                  pg_catalog.pg_class
                  ON (pg_inherits.inhrelid = pg_class.oid)
                INNER JOIN
                  pg_catalog.pg_namespace
                  ON (pg_class.relnamespace = pg_namespace.oid)
                WHERE
                  pg_class.relname = {table}
                  AND pg_namespace.nspname = {schema};
                """)
            )
            .format(
                table=psycopg.sql.Literal(table),
                schema=psycopg.sql.Literal(self.schema),
            )
            .as_string(self.conn)
        )
        return bool(self.cur.fetchone())

    def get_triggers(self, *, table: str) -> list[Trigger]:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  pg_trigger.tgname AS name,
                  pg_trigger.tgisinternal AS is_internal,
                  (
                    pg_trigger.tgname LIKE '%' || {name_prefix} || '%'
                    OR pg_trigger.tgname LIKE '%' || {repacked_name_prefix} || '%'
                  ) AS is_psycopack_trigger
                FROM
                  pg_catalog.pg_trigger
                INNER JOIN
                  pg_catalog.pg_class
                  ON (pg_class.oid = pg_trigger.tgrelid)
                INNER JOIN
                  pg_catalog.pg_namespace
                  ON (pg_class.relnamespace = pg_namespace.oid)
                WHERE
                  pg_class.relname = {table}
                  AND pg_namespace.nspname = {schema};
                """)
            )
            .format(
                table=psycopg.sql.Literal(table),
                schema=psycopg.sql.Literal(self.schema),
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
                  pg_attribute.attname AS column_name,
                  format_type(pg_attribute.atttypid, pg_attribute.atttypmod) AS data_type,
                  pg_attribute.attidentity AS identity_type
                FROM
                  pg_catalog.pg_index
                INNER JOIN
                  pg_catalog.pg_attribute
                  ON (
                    pg_attribute.attrelid = pg_index.indrelid
                    AND pg_attribute.attnum = ANY(pg_index.indkey)
                  )
                INNER JOIN
                  pg_catalog.pg_class
                  ON (pg_class.oid = pg_index.indrelid)
                INNER JOIN
                  pg_catalog.pg_namespace
                  ON (pg_class.relnamespace = pg_namespace.oid)
                WHERE
                  pg_class.relname = {table}
                  AND pg_namespace.nspname = {schema}
                  AND pg_index.indisprimary;
                """)
            )
            .format(
                table=psycopg.sql.Literal(table),
                schema=psycopg.sql.Literal(self.schema),
            )
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
                psycopg.sql.SQL(
                    "SELECT pg_get_serial_sequence({table_with_schema}, {column});"
                )
                .format(
                    table_with_schema=psycopg.sql.Literal(f"{self.schema}.{table}"),
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
                          pg_catalog.pg_get_expr(pg_attrdef.adbin, pg_attrdef.adrelid, true)
                        FROM
                          pg_catalog.pg_attrdef
                        WHERE
                          pg_attrdef.adrelid = pg_attribute.attrelid
                          AND pg_attrdef.adnum = pg_attribute.attnum
                          AND pg_attribute.atthasdef
                      ) AS seq_def
                    FROM
                      pg_catalog.pg_attribute
                    INNER JOIN
                      pg_catalog.pg_class
                      ON (pg_class.oid = pg_attribute.attrelid)
                    INNER JOIN
                      pg_catalog.pg_namespace
                      ON (pg_class.relnamespace = pg_namespace.oid)
                    WHERE
                      pg_class.relname = {table}
                      AND pg_namespace.nspname = {schema}
                      AND pg_attribute.attname = {column}
                    ORDER BY pg_attribute.attnum;
                    """)
                )
                .format(
                    table=psycopg.sql.Literal(table),
                    schema=psycopg.sql.Literal(self.schema),
                    column=psycopg.sql.Literal(pk_info.columns[0]),
                )
                .as_string(self.conn)
            )
            result = self.cur.fetchone()
            assert result is not None
            seq_def = result[0]
            if seq_def is None:
                return ""

            assert isinstance(seq_def, str)
            # The seq_def variable looks something like:
            #  nextval('psycopack_2999727_id_seq'::regclass)
            # Or when the schema is not "public":
            #  nextval('sweet_schema.psycopack_2999727_id_seq'::regclass)
            # So the nextval() parts and optional schema name are parsed out.
            start: int = seq_def.find("'") + 1
            end: int = seq_def.find("'", start)
            seq = seq_def[start:end]
            if "." in seq:
                seq = seq.split(".")[1]
            assert isinstance(seq, str)
            return seq

    def get_pk_sequence_value(self, *, seq: str) -> int:
        self.cur.execute(
            psycopg.sql.SQL("SELECT last_value FROM {schema}.{sequence};")
            .format(
                schema=psycopg.sql.Identifier(self.schema),
                sequence=psycopg.sql.Identifier(seq),
            )
            .as_string(self.conn)
        )
        result = self.cur.fetchone()
        assert result is not None
        value = result[0]
        assert isinstance(value, int)
        return value

    def get_pk_sequence_min_value(self, *, seq: str) -> int:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                    SELECT min_value FROM pg_sequences
                    WHERE schemaname = {schema} AND sequencename = {sequence};
                """)
            )
            .format(
                schema=psycopg.sql.Literal(self.schema),
                sequence=psycopg.sql.Literal(seq),
            )
            .as_string(self.conn)
        )
        result = self.cur.fetchone()
        assert result is not None
        value = result[0]
        assert isinstance(value, int)
        return value

    def get_backfill_batch(self, *, table: str) -> BackfillBatch | None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  id, batch_start, batch_end
                FROM
                  {schema}.{table}
                WHERE
                  finished IS false
                FOR UPDATE SKIP LOCKED
                LIMIT 1;
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )
        if not (result := self.cur.fetchone()):
            return None
        return BackfillBatch(id=result[0], start=result[1], end=result[2])

    def get_user(self) -> str:
        self.cur.execute(psycopg.sql.SQL("SELECT current_user;").as_string(self.conn))
        result = self.cur.fetchone()
        assert result is not None
        return str(result[0])

    def has_create_and_usage_privilege_on_schema(self) -> bool:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  has_schema_privilege({schema}, 'CREATE'),
                  has_schema_privilege({schema}, 'USAGE');
                """)
            )
            .format(schema=psycopg.sql.Literal(self.schema))
            .as_string(self.conn)
        )
        result = self.cur.fetchone()
        assert result is not None
        return bool(result[0] and result[1])

    def is_table_owner(self, *, table: str, schema: str) -> bool:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  tableowner = current_user OR tableowner IN (
                    SELECT
                      rolname
                    FROM
                      pg_roles
                    WHERE
                      pg_has_role(current_user, pg_roles.oid, 'MEMBER')
                    )
                FROM
                  pg_tables
                WHERE
                  schemaname = {schema}
                  AND tablename = {table}
                """)
            )
            .format(
                schema=psycopg.sql.Literal(schema),
                table=psycopg.sql.Literal(table),
            )
            .as_string(self.conn)
        )
        result = self.cur.fetchone()
        assert result is not None
        return bool(result[0])
