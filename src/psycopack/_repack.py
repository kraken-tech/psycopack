# TODO:
# 1. Due to the way the backfilling works, it may affect the correlation of a
#    certain field. TODO: Investigate if doing it the "repack" way is better in
#    such cases.
# 2. Add support for tables with fk in other schemas.
import datetime
import typing
from collections import defaultdict

from . import _commands, _cur, _identifiers, _introspect, _registry, _tracker
from . import _psycopg as psycopg


class BaseRepackError(Exception):
    pass


class TableDoesNotExist(BaseRepackError):
    pass


class TableIsEmpty(BaseRepackError):
    pass


class InheritedTable(BaseRepackError):
    pass


class TableHasTriggers(BaseRepackError):
    pass


class PrimaryKeyNotFound(BaseRepackError):
    pass


class CompositePrimaryKey(BaseRepackError):
    pass


class UnsupportedPrimaryKey(BaseRepackError):
    pass


class InvalidPrimaryKeyTypeForConversion(BaseRepackError):
    pass


class InvalidStageForReset(BaseRepackError):
    pass


class InvalidIndexes(BaseRepackError):
    pass


class ReferringForeignKeyInDifferentSchema(BaseRepackError):
    pass


class NoCreateAndUsagePrivilegeOnSchema(BaseRepackError):
    pass


class NotTableOwner(BaseRepackError):
    pass


class NoReferringTableOwnership(BaseRepackError):
    pass


class NoReferencesPrivilege(BaseRepackError):
    pass


class PostBackfillBatchCallback(typing.Protocol):
    def __call__(
        self, batch: _introspect.BackfillBatch, /
    ) -> None: ...  # pragma: no cover


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
        6. revert_swap(): Reverts the swap above. Useful if something went
           wrong.
        7. clean_up(): Drops the old table. This is non-recoverable. Make
           sure you only call this once you validated the table has been
           repacked adequately.

    - Additional: Reset routines.
        1. reset(): Drops all internal objects created by Psycopack and leaves
           the database in the same state it was before running any of the
           Psycopack functions above.
    """

    def __init__(
        self,
        *,
        table: str,
        batch_size: int,
        conn: psycopg.Connection,
        cur: psycopg.Cursor,
        convert_pk_to_bigint: bool = False,
        post_backfill_batch_callback: PostBackfillBatchCallback | None = None,
        lock_timeout: datetime.timedelta = datetime.timedelta(seconds=10),
        schema: str = "public",
    ) -> None:
        self.conn = conn
        self.cur = _cur.LoggedCursor(cur=cur)
        self.introspector = _introspect.Introspector(
            conn=self.conn,
            cur=self.cur,
            schema=schema,
        )
        self.command = _commands.Command(
            conn=self.conn,
            cur=self.cur,
            introspector=self.introspector,
            schema=schema,
        )

        self.table = table
        self.schema = schema
        self._check_table_exists()
        self._check_user_permissions()

        self.batch_size = batch_size
        self.post_backfill_batch_callback = post_backfill_batch_callback
        self.lock_timeout = lock_timeout
        self.convert_pk_to_bigint = convert_pk_to_bigint

        # Names for psycopack objects are stored in the Registry
        self.registry = _registry.Registry(
            conn=self.conn,
            cur=self.cur,
            schema=schema,
            introspector=self.introspector,
            command=self.command,
            table=table,
        )
        registry_row = self.registry.get_registry_row()
        self.copy_table = registry_row.copy_table
        self.id_seq = registry_row.id_seq
        self.function = registry_row.function
        self.trigger = registry_row.trigger
        self.backfill_log = registry_row.backfill_log
        # Names after the original table once it has been repacked and swapped.
        self.repacked_name = registry_row.repacked_name
        self.repacked_function = registry_row.repacked_function
        self.repacked_trigger = registry_row.repacked_trigger

        self.tracker = _tracker.Tracker(
            table=self.table,
            conn=self.conn,
            cur=self.cur,
            copy_table=self.copy_table,
            trigger=self.trigger,
            backfill_log=self.backfill_log,
            repacked_name=self.repacked_name,
            repacked_trigger=self.repacked_trigger,
            introspector=self.introspector,
            command=self.command,
            schema=schema,
        )
        self._pk_column = ""

    @property
    def pk_column(self) -> str:
        """
        Method to cache the name of the pk column in the instance as to avoid
        calling introspection queries multiple times.
        """
        if self._pk_column:
            return self._pk_column
        pk_info = self.introspector.get_primary_key_info(table=self.table)
        assert pk_info is not None
        assert len(pk_info.columns) == 1
        self._pk_column = pk_info.columns[0]
        return self._pk_column

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

            if self.introspector.is_inherited_table(table=self.table):
                raise InheritedTable("Psycopack does not support inherited tables.")

            pk_info = self.introspector.get_primary_key_info(table=self.table)

            if not pk_info:
                raise PrimaryKeyNotFound(
                    "Psycopack does not support tables without a primary key."
                )
            if len(pk_info.columns) > 1:
                raise CompositePrimaryKey(
                    "Psycopack does not support tables with composite primary keys."
                )
            supported_pk_data_types = (
                "bigint",
                "bigserial",
                "integer",
                "serial",
                "smallint",
                "smallserial",
            )
            pk_column = pk_info.columns[0]
            pk_data_type = pk_info.data_types[0]
            if pk_data_type not in supported_pk_data_types:
                raise UnsupportedPrimaryKey(
                    f"Psycopack only supports primary key columns called 'id' "
                    f"that are in the supported types: {supported_pk_data_types}. "
                    f"Found a column named: '{pk_column}' of type "
                    f"'{pk_data_type}' instead."
                )

            unsupported_triggers = [
                trigger
                for trigger in self.introspector.get_triggers(table=self.table)
                # Filter out any internal triggers that Postgres creates;
                # usually to enforce constraints, and also filter out the
                # triggers created by psycopack, as we can handle them
                # idempotently. If such triggers are present here the user
                # might have come from an interrupted/erroneous table
                # repacking.
                if (not trigger.is_internal and not trigger.is_psycopack_trigger)
            ]
            if any(unsupported_triggers):
                raise TableHasTriggers(
                    "Psycopack does not support table with triggers."
                )

            if self.convert_pk_to_bigint and "big" in pk_data_type:
                raise InvalidPrimaryKeyTypeForConversion(
                    f"Psycopack can't convert the table's primary key type "
                    f"from {pk_data_type} to a larger type."
                )

            invalid_indexes: list[str] = [
                i.name
                for i in self.introspector.get_index_def(table=self.table)
                if i.is_valid is False
            ]
            if invalid_indexes:
                raise InvalidIndexes(
                    f"Please either DROP or REINDEX the following indexes "
                    f"before proceeding with Psycopack: {invalid_indexes}."
                )

            fks_in_different_schema = [
                f"{fk.schema}.{fk.name}"
                for fk in self.introspector.get_referring_fks(table=self.table)
                if fk.schema != self.schema
            ]
            if len(fks_in_different_schema) > 0:
                raise ReferringForeignKeyInDifferentSchema(
                    f"Psycopack does not yet support tables with referring "
                    f"foreign keys in another schema. The table {self.table} "
                    f"has the following referring foreign keys in a different "
                    f"schema: {fks_in_different_schema}."
                )

    def setup_repacking(self) -> None:
        with (
            self.tracker.track(_tracker.Stage.SETUP),
            self.command.lock_timeout(self.lock_timeout),
        ):
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
            with self.command.lock_timeout(self.lock_timeout):
                self._create_unique_constraints()
                self._create_check_and_fk_constraints()
                self._create_referring_fks()

    def swap(self) -> None:
        """
        1. Drop the trigger (and function) that kept the copy table in sync.
        2. Swap the primary key sequence names.
        3. Rename the copy table (new) to the original name and the original
           (old) table to a new temp name.
        4. Create triggers from the new table to the old table to keep
           both in sync, in case the changes need to be reverted.
        """
        with self.tracker.track(_tracker.Stage.SWAP):
            with (
                self.command.db_transaction(),
                self.command.lock_timeout(self.lock_timeout),
            ):
                self.command.acquire_access_exclusive_lock(table=self.table)
                self.command.acquire_access_exclusive_lock(table=self.copy_table)
                self.command.drop_trigger_if_exists(
                    table=self.table, trigger=self.trigger
                )
                self.command.drop_function_if_exists(function=self.function)
                if self.introspector.get_pk_sequence_name(table=self.table):
                    self.command.swap_pk_sequence_name(
                        first_table=self.table, second_table=self.copy_table
                    )
                self.command.rename_table(
                    table_from=self.table, table_to=self.repacked_name
                )
                self.command.rename_table(
                    table_from=self.copy_table, table_to=self.table
                )
                self.command.create_copy_function(
                    function=self.repacked_function,
                    table_from=self.table,
                    table_to=self.repacked_name,
                    columns=self.introspector.get_table_columns(table=self.table),
                    pk_column=self.pk_column,
                )
                self.command.drop_trigger_if_exists(
                    table=self.table, trigger=self.repacked_trigger
                )
                self.command.create_copy_trigger(
                    trigger_name=self.repacked_trigger,
                    function=self.repacked_function,
                    table_from=self.table,
                    table_to=self.repacked_name,
                    pk_column=self.pk_column,
                )

    def revert_swap(self) -> None:
        """
        After calling swap(), this function can be called if any issues come up
        to revert back to using the original table instead of the repacked one.

        1. Drop the trigger (and function) that kept the original table in sync
           with the swapped-in copy.
        2. Swap the primary key sequence names.
        3. Rename the copy table back to its original copy name, and the
           original table back to its original name.
        4. Create triggers from the old table to the copy table to keep both in
           sync.
        """
        with (
            self.command.db_transaction(),
            self.command.lock_timeout(self.lock_timeout),
        ):
            self.tracker._revert_swap()
            self.command.acquire_access_exclusive_lock(table=self.table)
            self.command.acquire_access_exclusive_lock(table=self.repacked_name)
            self.command.drop_trigger_if_exists(
                table=self.table,
                trigger=self.repacked_trigger,
            )
            self.command.drop_function_if_exists(function=self.repacked_function)
            if self.introspector.get_pk_sequence_name(table=self.table):
                self.command.swap_pk_sequence_name(
                    first_table=self.table, second_table=self.repacked_name
                )
            self.command.rename_table(table_from=self.table, table_to=self.copy_table)
            self.command.rename_table(
                table_from=self.repacked_name, table_to=self.table
            )
            self._create_copy_function()
            self._create_copy_trigger()

    def clean_up(self) -> None:
        with self.tracker.track(_tracker.Stage.CLEAN_UP):
            # Rename indexes using a dict data structure to hold names from/to.
            # Also deal with duplicated indexes in the same column.
            indexes: dict[str, list[dict[str, str]]] = defaultdict(list)
            for idx in self.introspector.get_index_def(table=self.repacked_name):
                sql = idx.definition
                sql_arr = sql.split(" ON")
                sql = sql_arr[1].replace(self.repacked_name, self.table)
                indexes[sql].append({"idx_from": idx.name})

            for idx in self.introspector.get_index_def(table=self.table):
                sql = idx.definition
                sql_arr = sql.split(" ON")
                sql = sql_arr[1]
                idx_data = next(idx for idx in indexes[sql] if "idx_to" not in idx)
                idx_data["idx_to"] = idx.name

            # Rename foreign keys from other tables using a dict data structure to
            # hold names from/to.
            table_to_fk: dict[str, dict[str, str]] = {}
            for fk in self.introspector.get_referring_fks(table=self.repacked_name):
                table_to_fk[fk.referring_table] = {"cons_from": fk.name}

            for fk in self.introspector.get_referring_fks(table=self.table):
                table_to_fk[fk.referring_table]["cons_to"] = fk.name

            with (
                self.command.db_transaction(),
                self.command.lock_timeout(self.lock_timeout),
            ):
                self.command.drop_trigger_if_exists(
                    table=self.table, trigger=self.repacked_trigger
                )
                self.command.drop_function_if_exists(function=self.repacked_function)

                for idx_sql in indexes:
                    for index_data in indexes[idx_sql]:
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

                for table in table_to_fk:
                    fk_data = table_to_fk[table]
                    self.command.drop_constraint(
                        table=table,
                        constraint=fk_data["cons_from"],
                    )
                    self.command.rename_constraint(
                        table=table,
                        cons_from=fk_data["cons_to"],
                        cons_to=fk_data["cons_from"],
                    )

                self.command.drop_table_if_exists(table=self.repacked_name)
                self.command.drop_table_if_exists(table=self.backfill_log)
                self.registry.delete_row_for(table=self.table)

    def reset(self) -> None:
        current_stage = self.tracker.get_current_stage()
        if current_stage == _tracker.Stage.PRE_VALIDATION:
            raise InvalidStageForReset(
                "Psycopack hasn't run yet. There is no need to call reset."
            )
        if current_stage == _tracker.Stage.CLEAN_UP:
            raise InvalidStageForReset(
                f"Psycopack cannot reset from the CLEAN_UP stage. At this "
                f"point the table {self.table} has already been swapped and "
                f"cannot be reset. Try performing a revert_swap before trying "
                f"to reset the whole psycopack process."
            )

        with self.command.db_transaction():
            if self.introspector.get_table_oid(table=self.copy_table):
                fks = self.introspector.get_referring_fks(table=self.copy_table)
                for fk in fks:
                    self.command.drop_constraint(
                        table=fk.referring_table, constraint=fk.name
                    )
            self.command.drop_trigger_if_exists(table=self.table, trigger=self.trigger)
            self.command.drop_function_if_exists(function=self.function)
            self.command.drop_table_if_exists(table=self.backfill_log)
            self.command.drop_table_if_exists(table=self.copy_table)
            self.command.drop_sequence_if_exists(seq=self.id_seq)
            self.command.drop_table_if_exists(table=self.tracker.tracker_table)

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

        # The PK (and the implicit index created from it) are necessary for the
        # triggers to perform index lookups when writing to the table.
        self.command.add_pk(table=self.copy_table, pk_column=self.pk_column)

        if self.convert_pk_to_bigint:
            self.command.convert_pk_to_bigint(
                table=self.copy_table,
                seq=self.id_seq,
                pk_column=self.pk_column,
            )

        pk_info = self.introspector.get_primary_key_info(table=self.table)
        assert pk_info is not None and len(pk_info.columns) == 1

        if pk_info.identity_type:
            self.command.set_generated_identity(
                table=self.copy_table,
                always=(pk_info.identity_type == "a"),
                pk_column=self.pk_column,
            )
        elif self.introspector.get_pk_sequence_name(table=self.table):
            # Create a new sequence for the copied table's id column so that it
            # does not depend on the original's one. Otherwise, we wouldn't be
            # able to delete the original table after the repack process is
            # completed as it would have a dependency (the copy's table seq).
            self.command.drop_sequence_if_exists(seq=self.id_seq)
            self.command.create_sequence(
                seq=self.id_seq,
                bigint=("big" in pk_info.data_types[0].lower()),
            )
            self.command.set_table_id_seq(
                table=self.copy_table,
                seq=self.id_seq,
                pk_column=self.pk_column,
            )

        # If the original table has exclusion constraints, they need to be
        # replicated here when setting up the copy table. This is a limitation
        # from Postgres, as there is no way to create an exclusion constraint
        # ONLINE later when syncing the copy schema with the original table.
        exclusion_constraints = self.introspector.get_constraints(
            table=self.table,
            types=["x"],
        )
        for constraint in exclusion_constraints:
            self.command.create_constraint(
                table=self.copy_table,
                # Exclusion constraints are backed up by indexes, and because
                # indexes must have unique names, we need to name the exclusion
                # constraint with a temporary name here.
                name=_identifiers.build_postgres_identifier(
                    [constraint.name], "psycopack"
                ),
                definition=constraint.definition,
            )

    def _create_copy_function(self) -> None:
        self.command.drop_function_if_exists(function=self.function)
        self.command.create_copy_function(
            function=self.function,
            table_from=self.table,
            table_to=self.copy_table,
            columns=self.introspector.get_table_columns(table=self.table),
            pk_column=self.pk_column,
        )

    def _create_copy_trigger(self) -> None:
        self.command.drop_trigger_if_exists(table=self.table, trigger=self.trigger)
        self.command.create_copy_trigger(
            trigger_name=self.trigger,
            function=self.function,
            table_from=self.table,
            table_to=self.copy_table,
            pk_column=self.pk_column,
        )

    def _create_backfill_log(self) -> None:
        self.command.drop_table_if_exists(table=self.backfill_log)
        self.command.create_backfill_log(table=self.backfill_log)

    def _populate_backfill_log(self) -> None:
        min_pk, max_pk = self.introspector.get_min_and_max_pk(
            table=self.table,
            pk_column=self.pk_column,
        )
        self.command.populate_backfill_log(
            table=self.backfill_log,
            batch_size=self.batch_size,
            min_pk=min_pk,
            max_pk=max_pk,
        )

    def _perform_backfill(self) -> None:
        # TODO: This can be distributed across many workers instead of being
        # an infinite single-threaded loop.
        while True:
            with self.command.db_transaction():
                batch = self.introspector.get_backfill_batch(table=self.backfill_log)
                if not batch:
                    # No batches available to process at present.
                    break
                self.command.execute_copy_function(function=self.function, batch=batch)
                self.command.set_batch_to_finished(table=self.backfill_log, batch=batch)

            if self.post_backfill_batch_callback:
                self.post_backfill_batch_callback(batch)

    def _create_indexes(self) -> None:
        # Start by checking if there are any invalid indexes already created
        # due to a previous Psycopack run that failed midway through and delete
        # them. This excludes the primary index and exclusion constraints which
        # have been dealt with before this step.
        invalid_indexes = [
            index
            for index in self.introspector.get_index_def(table=self.copy_table)
            if (not index.is_primary)
            and (not index.is_exclusion)
            and (not index.is_valid)
        ]
        if invalid_indexes:
            with self.command.lock_timeout(datetime.timedelta(seconds=0)):
                for index in invalid_indexes:
                    self.command.drop_index_concurrently_if_exists(index=index.name)

        # We already created a PK index when creating the copy table, so we'll
        # skip it here as it does not need to be recreated. The same is true
        # for indexes servicing an exclusion constraint.
        indexes = [
            index
            for index in self.introspector.get_index_def(table=self.table)
            if (not index.is_primary) and (not index.is_exclusion)
        ]
        with self.command.lock_timeout(datetime.timedelta(seconds=0)):
            for index in indexes:
                name = index.name
                sql = index.definition
                sql = sql.replace(
                    "CREATE INDEX", "CREATE INDEX CONCURRENTLY IF NOT EXISTS"
                )
                sql = sql.replace(
                    "CREATE UNIQUE INDEX",
                    "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS",
                )
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

    def _create_unique_constraints(self) -> None:
        table_constraints = self.introspector.get_constraints(
            table=self.table, types=["u"]
        )
        copy_constraints = self.introspector.get_constraints(
            table=self.copy_table, types=["u"]
        )
        for cons in table_constraints:
            constraint_name = _identifiers.build_postgres_identifier(
                [cons.name], "psycopack"
            )
            if any(c.name == constraint_name for c in copy_constraints):
                # This constraint has already been created by a previous run.
                continue
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
        table_constraints = self.introspector.get_constraints(
            table=self.table, types=["c", "f"]
        )
        copy_constraints = self.introspector.get_constraints(
            table=self.copy_table, types=["c", "f"]
        )
        for cons in table_constraints:
            existing_cons = next(
                (c for c in copy_constraints if c.name == cons.name), None
            )
            if existing_cons and existing_cons.is_validated == cons.is_validated:
                # This constraint has already been created by a previous run
                # and exactly matches the constraint validation level from the
                # original table.
                continue
            if not existing_cons:
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
        table_fks = self.introspector.get_referring_fks(table=self.table)
        copy_fks = self.introspector.get_referring_fks(table=self.copy_table)

        for fk in table_fks:
            constraint_name = _identifiers.build_postgres_identifier(
                [fk.name], "psycopack"
            )
            existing_fk = next((f for f in copy_fks if f.name == constraint_name), None)
            if existing_fk and existing_fk.is_validated == fk.is_validated:
                # This constraint has already been created by a previous run
                # and exactly matches the constraint validation level from the
                # original table.
                continue
            if not existing_fk:
                if f"{self.schema}.{self.table}" in fk.definition:
                    definition = fk.definition.replace(
                        f"REFERENCES {self.schema}.{self.table}",
                        f"REFERENCES {self.schema}.{self.copy_table}",
                    )
                else:
                    definition = fk.definition.replace(
                        f"REFERENCES {self.table}",
                        f"REFERENCES {self.copy_table}",
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

    def _check_user_permissions(self) -> None:
        if not self.introspector.has_create_and_usage_privilege_on_schema():
            user = self.introspector.get_user()
            raise NoCreateAndUsagePrivilegeOnSchema(
                f"Psycopack requires the database user to have CREATE and USAGE "
                f"privilege on the {self.schema} schema to create auxiliary "
                f"objects. You can grant it to your user via:\n"
                f"GRANT CREATE, USAGE ON SCHEMA {self.schema} TO {user};"
            )

        if not self.introspector.is_table_owner(table=self.table, schema=self.schema):
            user = self.introspector.get_user()
            raise NotTableOwner(
                f"Psycopack requires the database user to have ownership of "
                f"the table {self.schema}.{self.table}. You can grant it to "
                f"your user via:\n"
                f"ALTER TABLE {self.schema}.{self.table} OWNER TO {user};"
            )

        referring_tables_without_ownership = [
            f"{fk.schema}.{fk.referring_table}"
            for fk in self.introspector.get_referring_fks(table=self.table)
            if not fk.is_owned_by_user
        ]
        if referring_tables_without_ownership:
            user = self.introspector.get_user()
            message = (
                f"Psycopack requires the user to have ownership of tables that "
                f"have foreign keys pointing to the table {self.schema}.{self.table}. "
                f"You can grant ownership to your user via the following commands:\n"
            )
            for table in referring_tables_without_ownership:
                message += f"ALTER TABLE {table} OWNER TO {user};\n"
            raise NoReferringTableOwnership(message)

        referred_fks_without_references_privilege = [
            fk
            for fk in self.introspector.get_referred_fks(
                table=self.table, schema=self.schema
            )
            if "REFERENCES" not in fk.privileges
        ]
        if referred_fks_without_references_privilege:
            user = self.introspector.get_user()
            tables_missing_ownership = [
                f"{fk.schema}.{fk.name}"
                for fk in referred_fks_without_references_privilege
            ]
            message = (
                f"Psycopack requires the user to REFERENCES privilege on tables that "
                f"{self.schema}.{self.table} has foreign keys to. "
                f"You can grant this privilege to your user via the following commands:\n"
            )
            for table in tables_missing_ownership:
                message += f"GRANT REFERENCES ON TABLE {table} TO {user};\n"
            raise NoReferencesPrivilege(message)

    def _check_table_exists(self) -> None:
        oid = self.introspector.get_table_oid(table=self.table)
        if oid is None:
            raise TableDoesNotExist(
                f'Table "{self.schema}.{self.table}" does not exist.'
            )
