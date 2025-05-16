import dataclasses
from textwrap import dedent

from . import _commands, _const, _cur, _introspect
from . import _psycopg as psycopg


@dataclasses.dataclass
class RegistryRow:
    original_table: str
    copy_table: str
    id_seq: str
    function: str
    trigger: str
    backfill_log: str
    repacked_name: str
    repacked_function: str
    repacked_trigger: str


class Registry:
    def __init__(
        self,
        *,
        conn: psycopg.Connection,
        cur: _cur.LoggedCursor,
        introspector: _introspect.Introspector,
        command: _commands.Command,
        schema: str,
        table: str,
    ) -> None:
        self.conn = conn
        self.cur = cur
        self.introspector = introspector
        self.schema = schema
        self.table = table

    def get_registry_row(self) -> RegistryRow:
        row = self._get_row_from_registry_table()
        if row:
            return row

        oid = self.introspector.get_table_oid(table=self.table)
        assert oid is not None

        copy_table = f"{_const.NAME_PREFIX}_{oid}"
        id_seq = f"{copy_table}_id_seq"
        function = f"{copy_table}_fun"
        trigger = f"{copy_table}_tgr"
        backfill_log = f"{copy_table}_backfill"
        repacked_name = copy_table.replace(
            _const.NAME_PREFIX, _const.REPACKED_NAME_PREFIX
        )
        repacked_function = function.replace(
            _const.NAME_PREFIX, _const.REPACKED_NAME_PREFIX
        )
        repacked_trigger = trigger.replace(
            _const.NAME_PREFIX, _const.REPACKED_NAME_PREFIX
        )
        row = RegistryRow(
            original_table=self.table,
            copy_table=copy_table,
            id_seq=id_seq,
            function=function,
            trigger=trigger,
            backfill_log=backfill_log,
            repacked_name=repacked_name,
            repacked_function=repacked_function,
            repacked_trigger=repacked_trigger,
        )
        self._insert_row_into_registry(row=row)
        return row

    def _get_row_from_registry_table(self) -> RegistryRow | None:
        self._ensure_registry_table_exists()
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  original_table,
                  copy_table,
                  id_seq,
                  function,
                  trigger,
                  backfill_log,
                  repacked_name,
                  repacked_function,
                  repacked_trigger
                FROM
                  {schema}.{registry_table}
                WHERE
                  original_table = {original_table}
                LIMIT 1;
                """)
            )
            .format(
                schema=psycopg.sql.Identifier(self.schema),
                registry_table=psycopg.sql.Identifier(_const.PSYCOPACK_REGISTRY),
                original_table=psycopg.sql.Literal(self.table),
            )
            .as_string(self.conn)
        )
        result = self.cur.fetchone()
        if not result:
            return None
        return RegistryRow(
            original_table=result[0],
            copy_table=result[1],
            id_seq=result[2],
            function=result[3],
            trigger=result[4],
            backfill_log=result[5],
            repacked_name=result[6],
            repacked_function=result[7],
            repacked_trigger=result[8],
        )

    def _ensure_registry_table_exists(self) -> None:
        if self._registry_table_exists():
            return
        self._create_registry_table()

    def _registry_table_exists(self) -> bool:
        return bool(self.introspector.get_table_oid(table=_const.PSYCOPACK_REGISTRY))

    def _create_registry_table(self) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                # - The maximum length for a Postgres identifier is 63.
                # - All names must be unique. Else, having the same name for
                #   two different tables being repacked would be ambiguous.
                dedent("""
                CREATE TABLE {schema}.{registry_table} (
                  original_table VARCHAR(63) NOT NULL UNIQUE,
                  copy_table VARCHAR(63) NOT NULL UNIQUE,
                  id_seq VARCHAR(63) NOT NULL UNIQUE,
                  function VARCHAR(63) NOT NULL UNIQUE,
                  trigger VARCHAR(63) NOT NULL UNIQUE,
                  backfill_log VARCHAR(63) NOT NULL UNIQUE,
                  repacked_name VARCHAR(63) NOT NULL UNIQUE,
                  repacked_function VARCHAR(63) NOT NULL UNIQUE,
                  repacked_trigger VARCHAR(63) NOT NULL UNIQUE
                );
                """)
            )
            .format(
                registry_table=psycopg.sql.Identifier(_const.PSYCOPACK_REGISTRY),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def _insert_row_into_registry(self, row: RegistryRow) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                INSERT INTO
                  {schema}.{registry_table}
                  (
                    original_table,
                    copy_table,
                    id_seq,
                    function,
                    trigger,
                    backfill_log,
                    repacked_name,
                    repacked_function,
                    repacked_trigger
                  )
                VALUES
                  (
                    {original_table},
                    {copy_table},
                    {id_seq},
                    {function},
                    {trigger},
                    {backfill_log},
                    {repacked_name},
                    {repacked_function},
                    {repacked_trigger}
                  );
                """)
            )
            .format(
                registry_table=psycopg.sql.Identifier(_const.PSYCOPACK_REGISTRY),
                schema=psycopg.sql.Identifier(self.schema),
                original_table=psycopg.sql.Literal(row.original_table),
                copy_table=psycopg.sql.Literal(row.copy_table),
                id_seq=psycopg.sql.Literal(row.id_seq),
                function=psycopg.sql.Literal(row.function),
                trigger=psycopg.sql.Literal(row.trigger),
                backfill_log=psycopg.sql.Literal(row.backfill_log),
                repacked_name=psycopg.sql.Literal(row.repacked_name),
                repacked_function=psycopg.sql.Literal(row.repacked_function),
                repacked_trigger=psycopg.sql.Literal(row.repacked_trigger),
            )
            .as_string(self.conn)
        )

    def delete_row_for(self, *, table: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                DELETE FROM
                  {schema}.{registry_table}
                WHERE
                  original_table = {original_table};
                """)
            )
            .format(
                schema=psycopg.sql.Identifier(self.schema),
                registry_table=psycopg.sql.Identifier(_const.PSYCOPACK_REGISTRY),
                original_table=psycopg.sql.Literal(table),
            )
            .as_string(self.conn)
        )
