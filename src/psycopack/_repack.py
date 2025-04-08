# TODO:
# 1. Tables using inheritance have not been tested and won't be supported for
#    the MVP. In future, repacking needs to raise an error if an attempt to
#    repack an inherited table is made.
# 2. In this PoC I assume the PK columns of the original and copy table are
#    called "id". In future, repacking to raise an error if this isn't the
#    case.
# 3. In this PoC I assume the original table doesn't have triggers to/from.
#    In future, repacking needs to raise an error if this isn't the case.
# 4. Exclusion constraints must be created before backfilling because they
#    can't be performed ONLINE. This MVP does not deal with exclusion
#    constraints yet.
# 5. The script doesn't take into consideration which schema the tables live
#    in. This only works if the default schema (public) is being used. In
#    future, this needs to be changed.
# 6. Due to the way the backfilling works, it may affect the correlation of a
#    certain field. TODO: Investigate if doing it the "repack" way is better in
#    such cases.
# 7. Add reasonable lock timeouts for operations and failure->retry routines.
# 8. Add reasonable lock_timeout values to prevent ACCESS EXCLUSIVE queries
#    blocking the queue by failing to acquire locks quickly.
from textwrap import dedent

from . import _commands, _cur, _identifiers, _introspect, _tracker
from . import _psycopg as psycopg


class BaseRepackError(Exception):
    pass


class TableDoesNotExist(BaseRepackError):
    pass


class TableIsEmpty(BaseRepackError):
    pass


class Repack:
    """
    Class for operating a full table repack.

    This class can be used in two different ways:

    - Calling Repack(...).full(): This will operate a repack from beginning to
      end without any intervention. This is a good choice if all you want is to
      repack a table.

    - Calling each of the public functions individually to pace-out or
      customise the repacking process. The public functions are:

        1. pre_validate(): Checks if the table can be repacked at all.
        2. setup_repacking(): Create the copy function, trigger, table, and
           also setup the backfill log table. The backfill log table controls
           the backfilling process. If you want to customise the layout of
           the final table, e.g., change the id from INT to BIGINT, or add an
           exclusion constraint, you can perform the required ALTER TABLE
           command after this step.
        3. backfill(): Performs the backfilling using the backfill log table
           to control backfilling.
        4. sync_schemas(): Recreates all the indexes, constraints, and
           referring foreign keys in the copy table. This step is performed
           after the backfill as to speed up writes against the table.
        5. swap(): This effectively swaps the tables. The original (now old)
           table will be kept in sync via triggers, in case the swap process
           needs to be reverted.
        6. [TODO] revert_swap(): Reverts the swap above. Useful if something
           went wrong.
        7. clean_up(): Drops the old table. This is non-recoverable. Make
           sure you only call this once you validated the table has been
           repacked adequately.
    """

    def __init__(
        self,
        *,
        table: str,
        batch_size: int,
        conn: psycopg.Connection,
        cur: psycopg.Cursor,
    ) -> None:
        self.conn = conn
        self.cur = _cur.LoggedCursor(cur=cur)
        self.introspector = _introspect.Introspector(conn=self.conn, cur=self.cur)
        self.command = _commands.Command(conn=self.conn, cur=self.cur)

        self.table = table
        self.batch_size = batch_size

        # Names for the copy table.
        self.copy_table = self._get_copy_table_name()
        self.id_seq = f"{self.copy_table}_id_seq"
        self.function = f"{self.copy_table}_fun"
        self.trigger = f"{self.copy_table}_tgr"
        self.backfill_log = f"{self.copy_table}_backfill"

        # Names after the original table once it has been repacked and swapped.
        self.repacked_name = self.copy_table.replace("repack", "repacked")
        self.repacked_function = self.function.replace("repack", "repacked")
        self.repacked_trigger = self.trigger.replace("repack", "repacked")

        self.tracker = _tracker.Tracker(
            table=self.table,
            conn=self.conn,
            cur=self.cur,
            copy_table=self.copy_table,
            trigger=self.trigger,
            backfill_log=self.backfill_log,
            repacked_name=self.repacked_name,
            repacked_trigger=self.repacked_trigger,
        )

    def full(self) -> None:
        """
        Process a full table repack from beginning to end.
        """
        stage = self.tracker.get_current_stage()
        if stage == _tracker.Stage.PRE_VALIDATION:
            self.pre_validate()
            self.setup_repacking()
            self.backfill()
            self.sync_schemas()
            self.swap()
            self.clean_up()

        if stage == _tracker.Stage.SETUP:
            self.setup_repacking()
            self.backfill()
            self.sync_schemas()
            self.swap()
            self.clean_up()

        if stage == _tracker.Stage.BACKFILL:
            self.backfill()
            self.sync_schemas()
            self.swap()
            self.clean_up()

        if stage == _tracker.Stage.SYNC_SCHEMAS:
            self.sync_schemas()
            self.swap()
            self.clean_up()

        if stage == _tracker.Stage.SWAP:
            self.swap()
            self.clean_up()

        if stage == _tracker.Stage.CLEAN_UP:
            self.clean_up()

    def pre_validate(self) -> None:
        with self.tracker.track(_tracker.Stage.PRE_VALIDATION):
            if self.introspector.table_is_empty(table=self.table):
                raise TableIsEmpty("No reason to repack an empty table.")

    def setup_repacking(self) -> None:
        with self.tracker.track(_tracker.Stage.SETUP):
            self._create_copy_table()
            self._create_copy_function()
            self._create_copy_trigger()
            self._create_backfill_log()
            self._populate_backfill_log()

    def backfill(self) -> None:
        with self.tracker.track(_tracker.Stage.BACKFILL):
            self._perform_backfill()

    def sync_schemas(self) -> None:
        with self.tracker.track(_tracker.Stage.SYNC_SCHEMAS):
            self._create_indexes()
            self._create_unique_constraints()
            self._create_check_and_fk_constraints()
            self._create_referring_fks()

    def _create_copy_table(self) -> None:
        # Checks if other relating objects have FKs pointing to the copy table
        # first. Deletes them (if any) as they might have been created by a
        # failed previous repacking process.
        if self.introspector.get_table_oid(table=self.copy_table) is not None:
            for fk in self.introspector.get_referring_fks(table=self.copy_table):
                self.command.drop_constraint(
                    table=fk.referring_table, constraint=fk.name
                )
            self.command.drop_table_if_exists(table=self.copy_table)

        self.command.create_copy_table(
            base_table=self.table, copy_table=self.copy_table
        )
        # Create a new sequence for the copied table's id column so that it
        # does not depend on the original's one. Otherwise, we wouldn't be able
        # to delete the original table after the repack process is completed
        # as it would have a dependency (the copy's table seq).
        self.command.drop_sequence_if_exists(seq=self.id_seq)
        self.command.create_sequence(seq=self.id_seq)
        self.command.set_table_id_seq(table=self.copy_table, seq=self.id_seq)
        # The PK (and the implicit index create from it) are necessary for the
        # triggers to perform index lookups when writing to the table.
        self.command.add_pk(table=self.copy_table)

    def _get_copy_table_name(self) -> str:
        oid = self.introspector.get_table_oid(table=self.table)
        if oid is None:
            raise TableDoesNotExist(f'Table "{self.table}" does not exist.')
        return f"repack_{oid}"

    def _create_copy_function(self) -> None:
        self.command.drop_function_if_exists(function=self.function)
        self.command.create_copy_function(
            function=self.function,
            table_from=self.table,
            table_to=self.copy_table,
            columns=self.introspector.get_table_columns(table=self.table),
        )

    def _create_copy_trigger(self) -> None:
        self.command.drop_trigger_if_exists(table=self.table, trigger=self.trigger)
        self.command.create_copy_trigger(
            trigger_name=self.trigger,
            function=self.function,
            table_from=self.table,
            table_to=self.copy_table,
        )

    def _create_backfill_log(self) -> None:
        self.command.drop_table_if_exists(table=self.backfill_log)
        self.command.create_backfill_log(table=self.backfill_log)

    def _populate_backfill_log(self) -> None:
        min_id, max_id = self.introspector.get_min_and_max_id(table=self.table)
        self.command.populate_backfill_log(
            table=self.backfill_log,
            batch_size=self.batch_size,
            min_id=min_id,
            max_id=max_id,
        )

    def _perform_backfill(self) -> None:
        # TODO: This can be distributed across many workers instead of being
        # an infinite single-threaded loop.
        while True:
            self.cur.execute("BEGIN;")
            self.cur.execute(
                psycopg.sql.SQL(
                    dedent("""
                    SELECT
                      id, batch_start, batch_end
                    FROM
                      {backfill_log}
                    WHERE
                      finished IS false
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1;
                    """)
                )
                .format(backfill_log=psycopg.sql.Identifier(self.backfill_log))
                .as_string(self.conn)
            )
            result = self.cur.fetchone()

            if not result:
                self.cur.execute("ABORT;")
                # No batches available to process at present.
                break

            id, batch_start, batch_end = result
            self.cur.execute(
                psycopg.sql.SQL("SELECT {function}({batch_start}, {batch_end});")
                .format(
                    function=psycopg.sql.Identifier(self.function),
                    batch_start=psycopg.sql.Literal(batch_start),
                    batch_end=psycopg.sql.Literal(batch_end),
                )
                .as_string(self.conn)
            )
            self.cur.execute(
                psycopg.sql.SQL(
                    dedent("""
                    UPDATE
                      {backfill_log}
                    SET
                      finished = true
                    WHERE
                      id = {id};
                    """)
                )
                .format(
                    backfill_log=psycopg.sql.Identifier(self.backfill_log),
                    id=psycopg.sql.Literal(id),
                )
                .as_string(self.conn)
            )
            self.cur.execute("COMMIT;")

    def _create_indexes(self) -> None:
        # We already created a PK index when creating the copy table, so we'll
        # skip it here as it does not need to be recreated.
        indexes = self.introspector.get_non_pk_index_def(table=self.table)
        self.cur.execute("SHOW lock_timeout;")
        result = self.cur.fetchone()
        assert result is not None
        lock_before = result[0]
        self.cur.execute("SET lock_timeout = '0';")
        for index in indexes:
            name = index.name
            sql = index.definition
            sql = sql.replace("CREATE INDEX", "CREATE INDEX CONCURRENTLY")
            sql = sql.replace("CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX CONCURRENTLY")
            sql_arr = sql.split(" ON")
            new_name = _identifiers.build_postgres_identifier([name], "psycopack")
            sql_arr[0] = sql_arr[0].replace(name, new_name)

            # Split further to handle when the column name is the same or
            # contains the table name in it.
            sql_arr_tmp = sql_arr[1].split("USING ")
            sql_arr_tmp[0] = sql_arr_tmp[0].replace(self.table, self.copy_table)
            sql_arr[1] = "USING ".join(sql_arr_tmp)

            sql = " ON ".join(sql_arr)
            self.cur.execute(sql)
        self.cur.execute(f"SET lock_timeout = '{lock_before}';")

    def _create_unique_constraints(self) -> None:
        for cons in self.introspector.get_constraints(table=self.table, types=["u"]):
            constraint_name = _identifiers.build_postgres_identifier(
                [cons.name], "psycopack"
            )
            if not cons.is_validated:  # pragma: no cover
                # TODO: Gotta handle that case later
                continue
            self.command.create_unique_constraint_using_idx(
                table=self.copy_table,
                constraint=constraint_name,
                # From previous steps, the index name is the same as the
                # constraint, not a typo!
                index=constraint_name,
                is_deferrable=cons.is_deferrable,
                is_deferred=cons.is_deferred,
            )

    def _create_check_and_fk_constraints(self) -> None:
        for cons in self.introspector.get_constraints(
            table=self.table,
            types=["c", "f"],
        ):
            self.command.create_not_valid_constraint_from_def(
                table=self.copy_table,
                constraint=cons.name,
                definition=cons.definition,
                is_validated=cons.is_validated,
            )
            if cons.is_validated:
                self.command.validate_constraint(
                    table=self.copy_table, constraint=cons.name
                )

    def _create_referring_fks(self) -> None:
        for fk in self.introspector.get_referring_fks(table=self.table):
            definition = fk.definition.replace(
                f"REFERENCES {self.table}", f"REFERENCES {self.copy_table}"
            )
            constraint_name = _identifiers.build_postgres_identifier(
                [fk.name], "psycopack"
            )
            self.command.create_not_valid_constraint_from_def(
                table=fk.referring_table,
                constraint=constraint_name,
                definition=definition,
                is_validated=fk.is_validated,
            )
            if fk.is_validated:
                self.command.validate_constraint(
                    table=fk.referring_table, constraint=constraint_name
                )

    def swap(self) -> None:
        """
        1. Drop the trigger (and function) that kept the copy table in sync.
        2. Rename the copy table (new) to the original name and the original
           (old) table to a new temp name.
        3. Create triggers from the new table to the old table to keep
           both in sync, in case the changes need to be reverted.
        """
        with self.tracker.track(_tracker.Stage.SWAP):
            self.cur.execute("BEGIN;")
            self.cur.execute(f"LOCK TABLE {self.table} IN ACCESS EXCLUSIVE MODE;")
            self.cur.execute(f"LOCK TABLE {self.copy_table} IN ACCESS EXCLUSIVE MODE;")
            self.command.drop_trigger_if_exists(table=self.table, trigger=self.trigger)
            self.command.drop_function_if_exists(function=self.function)
            self.command.rename_table(
                table_from=self.table, table_to=self.repacked_name
            )
            self.command.rename_table(table_from=self.copy_table, table_to=self.table)
            self.command.create_copy_function(
                function=self.repacked_function,
                table_from=self.table,
                table_to=self.repacked_name,
                columns=self.introspector.get_table_columns(table=self.table),
            )
            self.command.drop_trigger_if_exists(
                table=self.table, trigger=self.repacked_trigger
            )
            self.command.create_copy_trigger(
                trigger_name=self.repacked_trigger,
                function=self.repacked_function,
                table_from=self.table,
                table_to=self.repacked_name,
            )
            self.cur.execute("COMMIT;")

    def clean_up(self) -> None:
        with self.tracker.track(_tracker.Stage.CLEAN_UP):
            self.command.drop_trigger_if_exists(
                table=self.table, trigger=self.repacked_trigger
            )
            self.command.drop_function_if_exists(function=self.repacked_function)

            # Rename indexes using a dict data structure to hold names from/to.
            indexes: dict[str, dict[str, str]] = {}
            for idx in self.introspector.get_index_def(table=self.repacked_name):
                sql = idx.definition
                sql_arr = sql.split(" ON")
                sql = sql_arr[1].replace(self.repacked_name, self.table)
                indexes[sql] = {"idx_from": idx.name}

            for idx in self.introspector.get_index_def(table=self.table):
                sql = idx.definition
                sql_arr = sql.split(" ON")
                sql = sql_arr[1]
                indexes[sql]["idx_to"] = idx.name

            for idx_sql in indexes:
                index_data = indexes[idx_sql]
                self.command.rename_index(
                    idx_from=index_data["idx_from"],
                    idx_to=_identifiers.build_postgres_identifier(
                        [index_data["idx_from"]], "idx_from"
                    ),
                )
                self.command.rename_index(
                    idx_from=index_data["idx_to"],
                    idx_to=index_data["idx_from"],
                )

            # Rename foreign keys from other tables using a dict data structure to
            # hold names from/to.
            table_to_fk: dict[str, dict[str, str]] = {}
            for fk in self.introspector.get_referring_fks(table=self.repacked_name):
                table_to_fk[fk.referring_table] = {"cons_from": fk.name}

            for fk in self.introspector.get_referring_fks(table=self.table):
                table_to_fk[fk.referring_table]["cons_to"] = fk.name

            for table in table_to_fk:
                fk_data = table_to_fk[table]
                self.command.rename_constraint(
                    table=table,
                    cons_from=fk_data["cons_from"],
                    cons_to=_identifiers.build_postgres_identifier(
                        [fk_data["cons_from"]], "const_from"
                    ),
                )
                self.command.rename_constraint(
                    table=table,
                    cons_from=fk_data["cons_to"],
                    cons_to=fk_data["cons_from"],
                )

            for fk in self.introspector.get_referring_fks(table=self.repacked_name):
                self.command.drop_constraint(
                    table=fk.referring_table, constraint=fk.name
                )

            self.command.drop_table_if_exists(table=self.repacked_name)
            self.command.drop_table_if_exists(table=self.backfill_log)
