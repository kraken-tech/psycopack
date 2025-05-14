import dataclasses
from textwrap import dedent
from unittest import mock

import pytest

from psycopack import (
    BackfillBatch,
    CompositePrimaryKey,
    FailureDueToLockTimeout,
    InheritedTable,
    InvalidIndexes,
    InvalidPrimaryKeyTypeForConversion,
    InvalidStageForReset,
    NoCreateAndUsagePrivilegeOnSchema,
    NotTableOwner,
    PrimaryKeyNotFound,
    ReferringForeignKeyInDifferentSchema,
    Repack,
    TableDoesNotExist,
    TableHasTriggers,
    TableIsEmpty,
    UnsupportedPrimaryKey,
    _cur,
    _introspect,
    _psycopg,
    _tracker,
)
from tests import factories


@dataclasses.dataclass
class _TableInfo:
    oid: int
    indexes: list[_introspect.Index]
    referring_fks: list[_introspect.ReferringForeignKey]
    constraints: list[_introspect.Constraint]
    pk_seq: str


def _collect_table_info(
    table: str,
    connection: _psycopg.Connection,
    schema: str = "public",
) -> _TableInfo:
    with connection.cursor() as cur:
        introspector = _introspect.Introspector(
            conn=connection,
            cur=_cur.LoggedCursor(cur=cur),
            schema=schema,
        )
        oid = introspector.get_table_oid(table=table)
        assert oid is not None
        indexes = introspector.get_index_def(table=table)
        referring_fks = introspector.get_referring_fks(table=table)
        constraints = introspector.get_constraints(
            table=table, types=["c", "f", "n", "p", "u", "t", "x"]
        )
        pk_seq = introspector.get_pk_sequence_name(table=table)

    return _TableInfo(
        oid=oid,
        indexes=indexes,
        referring_fks=referring_fks,
        constraints=constraints,
        pk_seq=pk_seq,
    )


@dataclasses.dataclass
class _TriggerInfo:
    trigger_exists: bool
    repacked_trigger_exists: bool


def _get_trigger_info(repack: Repack, cur: _psycopg.Cursor) -> _TriggerInfo:
    cur.execute(f"SELECT 1 FROM pg_trigger WHERE tgname = '{repack.trigger}'")
    trigger_exists = cur.fetchone() is not None
    cur.execute(f"SELECT 1 FROM pg_trigger WHERE tgname = '{repack.repacked_trigger}'")
    repacked_trigger_exists = cur.fetchone() is not None
    return _TriggerInfo(
        trigger_exists=trigger_exists, repacked_trigger_exists=repacked_trigger_exists
    )


@dataclasses.dataclass
class _FunctionInfo:
    function_exists: bool
    repacked_function_exists: bool


def _get_function_info(repack: Repack, cur: _psycopg.Cursor) -> _FunctionInfo:
    cur.execute(f"SELECT 1 FROM pg_proc WHERE proname = '{repack.function}'")
    function_exists = cur.fetchone() is not None
    cur.execute(f"SELECT 1 FROM pg_proc WHERE proname = '{repack.repacked_function}'")
    repacked_function_exists = cur.fetchone() is not None
    return _FunctionInfo(
        function_exists=function_exists,
        repacked_function_exists=repacked_function_exists,
    )


@dataclasses.dataclass
class _SequenceInfo:
    sequence_exists: bool


def _get_sequence_info(repack: Repack, cur: _psycopg.Cursor) -> _SequenceInfo:
    cur.execute(f"SELECT 1 FROM pg_sequences WHERE sequencename = '{repack.id_seq}'")
    sequence_exists = cur.fetchone() is not None
    return _SequenceInfo(sequence_exists=sequence_exists)


def _assert_repack(
    table_before: _TableInfo,
    table_after: _TableInfo,
    repack: Repack,
    cur: _psycopg.Cursor,
) -> None:
    # They aren't the same tables (thus different oids), but everything else is
    # the same.
    assert table_before.oid != table_after.oid
    assert table_before.indexes == table_after.indexes
    assert table_before.referring_fks == table_after.referring_fks
    assert table_before.constraints == table_after.constraints
    assert table_before.pk_seq == table_after.pk_seq

    # All functions and triggers are removed.
    trigger_info = _get_trigger_info(repack, cur)
    assert trigger_info.trigger_exists is False
    assert trigger_info.repacked_trigger_exists is False
    function_info = _get_function_info(repack, cur)
    assert function_info.function_exists is False
    assert function_info.repacked_function_exists is False

    # The tracker table itself will also be deleted after the clean up process.
    assert repack.introspector.get_table_oid(table=repack.tracker.tracker_table) is None


def _assert_reset(repack: Repack, cur: _psycopg.Cursor) -> None:
    assert _get_trigger_info(repack, cur).trigger_exists is False
    assert _get_function_info(repack, cur).function_exists is False
    assert _get_sequence_info(repack, cur).sequence_exists is False
    assert repack.introspector.get_table_oid(table=repack.copy_table) is None
    assert repack.introspector.get_table_oid(table=repack.tracker.tracker_table) is None


@pytest.mark.parametrize(
    "pk_type",
    ("bigint", "bigserial", "integer", "serial", "smallint", "smallserial"),
)
def test_repack_full(connection: _psycopg.Connection, pk_type: str) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type=pk_type,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


@pytest.mark.parametrize(
    "pk_type,identity_type,convert_pk_to_bigint,expected_seq_max_val",
    (
        ("integer generated always as identity", "a", False, 2147483647),
        ("integer generated by default as identity", "d", False, 2147483647),
        ("integer generated always as identity", "a", True, 9223372036854775807),
        ("integer generated by default as identity", "d", True, 9223372036854775807),
    ),
)
def test_repack_full_with_identity_pk(
    connection: _psycopg.Connection,
    pk_type: str,
    identity_type: str,
    convert_pk_to_bigint: bool,
    expected_seq_max_val: int,
) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type=pk_type,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
            convert_pk_to_bigint=convert_pk_to_bigint,
        )
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )
        pk_info = repack.introspector.get_primary_key_info(table="to_repack")
        assert pk_info is not None
        assert pk_info.identity_type == identity_type

        cur.execute(
            f"""
            SELECT
              1
            FROM
              pg_sequences
            WHERE
              sequencename = split_part(pg_get_serial_sequence('to_repack', 'id'), '.', 2)
              AND max_value = {expected_seq_max_val};
            """
        )
        assert cur.fetchone()


def test_when_table_does_not_exist(connection: _psycopg.Connection) -> None:
    with connection.cursor() as cur:
        with pytest.raises(TableDoesNotExist):
            Repack(
                table="to_repack",
                batch_size=1,
                conn=connection,
                cur=cur,
            ).full()


def test_when_table_is_empty(connection: _psycopg.Connection) -> None:
    with connection.cursor() as cur:
        cur.execute("CREATE TABLE empty_table (id SERIAL PRIMARY KEY);")

        with pytest.raises(TableIsEmpty):
            Repack(
                table="empty_table",
                batch_size=1,
                conn=connection,
                cur=cur,
            ).full()


def test_repack_full_after_pre_validate_called(connection: _psycopg.Connection) -> None:
    """
    full() should be able to be called no matter where the repacking process
    left out from.

    In this case, it will just proceed from the pre_validation step.
    """
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_repack_full_after_setup_called(connection: _psycopg.Connection) -> None:
    """
    full() should be able to be called no matter where the repacking process
    left out from.

    In this case, it will remove the copy table, function, and trigger that
    already exist due to the setup_repacking() method being called beforehand.
    """
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        repack.setup_repacking()
        # Setup finished. Next stage is backfill.
        assert repack.tracker.get_current_stage() == _tracker.Stage.BACKFILL
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_repack_full_after_backfill(connection: _psycopg.Connection) -> None:
    """
    full() should be able to be called no matter where the repacking process
    left out from.

    In this case, it will drop the backfill log table and all related tables
    to repack and repack again from scratch.
    """
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        # Backfill finished. Next stage is sync_schemas.
        assert repack.tracker.get_current_stage() == _tracker.Stage.SYNC_SCHEMAS
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_repack_full_after_sync_schemas_called(connection: _psycopg.Connection) -> None:
    """
    full() should be able to be called no matter where the repacking process
    left out from.

    In this case, it will remove the foreign keys from referring tables that
    were created by the setup_repacking() method.
    """
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        # Sync schemas finished. Next stage is swap.
        assert repack.tracker.get_current_stage() == _tracker.Stage.SWAP
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_repack_full_after_swap_called(connection: _psycopg.Connection) -> None:
    """
    full() should be able to be called no matter where the repacking process
    left out from.

    In this case, it will pick up from the swap operation, which swaps the copy
    table for the original and creates a new trigger to keep the original table
    updated with new inserts into the copy.
    """
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        repack.swap()
        # Swap finished. Next stage is clean up.
        assert repack.tracker.get_current_stage() == _tracker.Stage.CLEAN_UP
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_clean_up_finishes_the_repacking(connection: _psycopg.Connection) -> None:
    """
    The last step on the full() method is to perform a clean_up(). That means
    that the repacking should be finished up as soon as clean_up() returns.
    """
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        repack.swap()
        repack.clean_up()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_sync_schemas_is_reentrant_and_idempotent(
    connection: _psycopg.Connection,
) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()

        # Add an invalid index to verify it will be cleaned up.
        cur.execute(
            f"CREATE INDEX btree_idx_psycopack ON {repack.copy_table} (var_with_btree);"
        )
        cur.execute("""
            UPDATE
              pg_index
            SET
              indisvalid = false
            WHERE
              indexrelid = 'btree_idx_psycopack'::regclass;
        """)
        invalid_indexes = [
            index
            for index in repack.introspector.get_index_def(table=repack.copy_table)
            if not index.is_valid
        ]
        assert len(invalid_indexes) == 1
        assert invalid_indexes[0].name == "btree_idx_psycopack"

        with mock.patch.object(repack, "_create_unique_constraints") as mocked:
            # An expection on _create_unique_constraints means that all indexes
            # would've been created already. So the second run should pick up
            # those indexes instead of recreating them.
            mocked.side_effect = Exception("BANG")
            with pytest.raises(Exception, match="BANG"):
                repack.sync_schemas()

        with mock.patch.object(repack, "_create_check_and_fk_constraints") as mocked:
            # An expection on _create_check_and_fk_constraints means that all
            # unique constraints would've been created already. So the second
            # run should pick up those constraints instead of recreating them.
            mocked.side_effect = Exception("BAANG")
            with pytest.raises(Exception, match="BAANG"):
                repack.sync_schemas()

        with mock.patch.object(repack.command, "validate_constraint") as mocked:
            # An exception on validate_constraint means that one of the check
            # constraints wouldn't be validated and the process would fail
            # right there. This would leave a NOT VALID constraint behind. This
            # should be able to be fixed next time sync_schema() runs.
            mocked.side_effect = Exception("BAAANG")
            with pytest.raises(Exception, match="BAAANG"):
                repack.sync_schemas()

        with mock.patch.object(repack, "_create_referring_fks") as mocked:
            # An expection on _create_referring_fks means that all check and fk
            # constraints would've been created already. So the second run
            # should pick up those constraints instead of recreating them.
            mocked.side_effect = Exception("BAAAANG")
            with pytest.raises(Exception, match="BAAAANG"):
                repack.sync_schemas()

        with mock.patch.object(repack.command, "validate_constraint") as mocked:
            # An exception on validate_constraint means that one of the
            # referring fks wouldn't be validated and the process would fail
            # right there. This would leave a NOT VALID fk constraint behind.
            # This should be able to be fixed next time sync_schema() runs.
            mocked.side_effect = Exception("BAAAAANG")
            with pytest.raises(Exception, match="BAAAAANG"):
                repack.sync_schemas()

        repack.sync_schemas()
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )
        # No invalid indexes left behind in the process.
        assert not any(
            [
                index
                for index in repack.introspector.get_index_def(table="to_repack")
                if not index.is_valid
            ]
        )


def test_when_tracker_removed_after_sync_schemas(
    connection: _psycopg.Connection,
) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )

        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        assert repack.tracker.get_current_stage() == _tracker.Stage.SWAP

        # Deleting the tracker table will remove the ability to pick up the
        # repacking process where it left. But nonetheless, it should resume
        # happily from scratch.
        repack.command.drop_table_if_exists(table=repack.tracker.tracker_table)

        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_when_tracker_table_current_row_is_deleted(
    connection: _psycopg.Connection,
) -> None:
    """
    A sane error must be returned and the repacking process must be stopped if
    the tracker table has been manipulated.

    In this case, the row containing the current stage has been deleted
    manually.
    """
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )

        repack.pre_validate()
        repack.setup_repacking()
        assert repack.tracker.get_current_stage() == _tracker.Stage.BACKFILL

        cur.execute(
            f"DELETE FROM {repack.tracker.tracker_table} WHERE stage = 'BACKFILL';"
        )
        with pytest.raises(_tracker.CannotFindUnfinishedStage):
            repack.full()


def test_when_rogue_row_inserted_in_tracker_table(
    connection: _psycopg.Connection,
) -> None:
    """
    A sane error must be returned and the repacking process must be stopped if
    the tracker table has been manipulated.

    In this case, a new row that shouldn't be there has been inserted into the
    table manually.
    """
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )

        repack.pre_validate()
        repack.setup_repacking()
        assert repack.tracker.get_current_stage() == _tracker.Stage.BACKFILL

        cur.execute(
            _psycopg.sql.SQL(
                """
                INSERT INTO
                  {table} (stage, step, started_at, finished_at)
                VALUES
                  ({stage}, {step}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                """
            )
            .format(
                table=_psycopg.sql.Identifier(repack.tracker.tracker_table),
                stage=_psycopg.sql.Literal("A stage that doesn't exist"),
                step=_psycopg.sql.Literal(42),
            )
            .as_string(connection)
        )
        with pytest.raises(_tracker.InvalidRowInTrackerTable):
            repack.full()


def test_table_to_repack_deleted_after_pre_validation(
    connection: _psycopg.Connection,
) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )

        repack.pre_validate()
        assert repack.tracker.get_current_stage() == _tracker.Stage.SETUP

        cur.execute("DROP TABLE to_repack CASCADE;")
        with pytest.raises(_tracker.TableDoesNotExist):
            repack.full()


def test_trigger_deleted_after_setup(connection: _psycopg.Connection) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        repack.setup_repacking()
        assert repack.tracker.get_current_stage() == _tracker.Stage.BACKFILL

        repack.command.drop_trigger_if_exists(
            table=repack.table, trigger=repack.trigger
        )
        with pytest.raises(_tracker.TriggerDoesNotExist):
            repack.full()


def test_cannot_repeat_finished_stage(connection: _psycopg.Connection) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        with pytest.raises(_tracker.StageAlreadyFinished):
            repack.pre_validate()

        repack.setup_repacking()
        with pytest.raises(_tracker.StageAlreadyFinished):
            repack.setup_repacking()

        repack.backfill()
        with pytest.raises(_tracker.StageAlreadyFinished):
            repack.backfill()

        repack.sync_schemas()
        with pytest.raises(_tracker.StageAlreadyFinished):
            repack.sync_schemas()

        repack.swap()
        with pytest.raises(_tracker.StageAlreadyFinished):
            repack.swap()

        repack.clean_up()
        with pytest.raises(_tracker.InvalidRepackingSetup):
            # The clean up process above already deleted all repacking
            # relations, including the tracker table. So trying to call it
            # again indicates trying to start repacking from scratch straight
            # from the clean up stage, which is invalid.
            repack.clean_up()

        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_cannot_skip_order_of_stages(connection: _psycopg.Connection) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        with pytest.raises(_tracker.InvalidRepackingSetup):
            # Can't initialise a repacking without going through pre-validation
            # first.
            repack.setup_repacking()

        repack.pre_validate()

        with pytest.raises(_tracker.InvalidRepackingStep):
            # Can't go to backfill without setting up first.
            repack.backfill()

        repack.setup_repacking()

        with pytest.raises(_tracker.InvalidRepackingStep):
            # Can't go to sync schemas without backfilling first
            repack.sync_schemas()

        repack.backfill()

        with pytest.raises(_tracker.InvalidRepackingStep):
            # Can't go to swap without syncing schemas first.
            repack.swap()

        repack.sync_schemas()
        with pytest.raises(_tracker.InvalidRepackingStep):
            # Can't go to clean up without swapping first.
            repack.clean_up()

        repack.swap()
        repack.clean_up()

        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


@pytest.mark.parametrize("ommit_sequence", (True, False))
def test_revert_swap_after_swap_called(
    connection: _psycopg.Connection, ommit_sequence: bool
) -> None:
    """
    The revert_swap() routine can only be called immediatelly after swap().

    This routine should leave the repacking status exactly the same way it was
    before swap() was called.
    """
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type="integer",
            ommit_sequence=ommit_sequence,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()

        repack.swap()
        # After the swap, repacking is ready for the clean-up stage.
        assert repack.tracker.get_current_stage() == _tracker.Stage.CLEAN_UP
        table_after_swap = _collect_table_info(table="to_repack", connection=connection)
        assert table_before.oid != table_after_swap.oid

        repack.revert_swap()
        table_after_revert = _collect_table_info(
            table="to_repack", connection=connection
        )
        assert table_before.oid == table_after_revert.oid
        # After the revert swap, repacking is ready for the swap stage.
        assert repack.tracker.get_current_stage() == _tracker.Stage.SWAP

        # We can run swap again now, without any errors.
        repack.swap()

        # Finally finish the repacking process.
        repack.full()

        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_revert_swap_before_swap_called(connection: _psycopg.Connection) -> None:
    """
    The revert_swap() routine can only be called immediatelly after swap().

    This test tries to call revert_swap() _before_ swap() has been called,
    which is wrong.
    """
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()

        with pytest.raises(_tracker.CannotRevertSwap):
            repack.revert_swap()


def test_repack_with_exclusion_constraint(connection: _psycopg.Connection) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            with_exclusion_constraint=True,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        repack.setup_repacking()
        # At this stage, the exclusion constraint is in the copy table.
        assert (
            len(
                repack.introspector.get_constraints(
                    table=repack.copy_table, types=["x"]
                )
            )
            == 1
        )
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_repack_with_inherited_table(connection: _psycopg.Connection) -> None:
    with connection.cursor() as cur:
        cur.execute("CREATE TABLE parent (id SERIAL PRIMARY KEY);")
        cur.execute("CREATE TABLE child () INHERITS (parent);")
        # Insert a row so that the table passes the TableIsEmpty check.
        cur.execute("INSERT INTO child (id) SELECT generate_series(1, 1);")
        repack = Repack(
            table="child",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        with pytest.raises(InheritedTable):
            repack.full()


def test_repack_when_table_has_triggers(connection: _psycopg.Connection) -> None:
    with connection.cursor() as cur:
        cur.execute("CREATE TABLE table_with_triggers (id SERIAL PRIMARY KEY);")
        # Insert a row so that the table passes the TableIsEmpty check.
        cur.execute(
            "INSERT INTO table_with_triggers (id) SELECT generate_series(1, 1);"
        )
        cur.execute("""
            CREATE OR REPLACE FUNCTION log_insert()
            RETURNS TRIGGER AS $$
            BEGIN
                RAISE NOTICE 'New row inserted with ID: %', NEW.id;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER after_insert_trigger
            AFTER INSERT ON table_with_triggers
            FOR EACH ROW
            EXECUTE FUNCTION log_insert();
        """)
        repack = Repack(
            table="table_with_triggers",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        with pytest.raises(TableHasTriggers):
            repack.full()


def test_table_without_pk(connection: _psycopg.Connection) -> None:
    with connection.cursor() as cur:
        cur.execute("CREATE TABLE table_without_fk (id integer);")
        # Insert a row so that the table passes the TableIsEmpty check.
        cur.execute("INSERT INTO table_without_fk (id) VALUES (42)")
        repack = Repack(
            table="table_without_fk",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        with pytest.raises(PrimaryKeyNotFound):
            repack.full()


def test_table_without_supported_pk_type(connection: _psycopg.Connection) -> None:
    with connection.cursor() as cur:
        cur.execute("CREATE TABLE table_with_var_pk (id varchar PRIMARY KEY);")
        # Insert a row so that the table passes the TableIsEmpty check.
        cur.execute("INSERT INTO table_with_var_pk (id) VALUES ('gday')")
        repack = Repack(
            table="table_with_var_pk",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        with pytest.raises(UnsupportedPrimaryKey):
            repack.full()


def test_with_pk_name_different_than_id(connection: _psycopg.Connection) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_name="a_name_that_is_not_id",
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_table_with_composite_pk(connection: _psycopg.Connection) -> None:
    with connection.cursor() as cur:
        cur.execute(
            # The first column is called "id" on purpose, even though it is not
            # the single column in the PK definition. This is to guarantee the
            # check won't pass simply because the column  name is "id".
            """
            CREATE TABLE composite_pk (
                id integer,
                id_2 varchar,
                PRIMARY KEY (id, id_2)
            );
            """
        )
        # Insert a row so that the table passes the TableIsEmpty check.
        cur.execute("INSERT INTO composite_pk (id, id_2) VALUES (1, 'hey');")
        repack = Repack(
            table="composite_pk",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        with pytest.raises(CompositePrimaryKey):
            repack.full()


@pytest.mark.parametrize("pk_type", ("bigint", "bigserial"))
def test_table_with_invalid_primary_key_type_to_enlarge(
    connection: _psycopg.Connection, pk_type: str
) -> None:
    with connection.cursor() as cur:
        cur.execute(f"CREATE TABLE table_with_big_pk (id {pk_type} PRIMARY KEY);")
        # Insert a row so that the table passes the TableIsEmpty check.
        cur.execute("INSERT INTO table_with_big_pk (id) VALUES (42)")
        repack = Repack(
            table="table_with_big_pk",
            batch_size=1,
            conn=connection,
            cur=cur,
            convert_pk_to_bigint=True,
        )
        with pytest.raises(InvalidPrimaryKeyTypeForConversion):
            repack.full()


@pytest.mark.parametrize(
    "initial_pk_type",
    (
        "integer",
        "serial",
        "smallint",
        "smallserial",
    ),
)
def test_with_primary_key_enlargement(
    connection: _psycopg.Connection, initial_pk_type: str
) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type=initial_pk_type,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
            convert_pk_to_bigint=True,
        )
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )
        pk_info = repack.introspector.get_primary_key_info(table="to_repack")
        assert pk_info and pk_info.data_types[0] == "bigint"
        cur.execute(
            f"""
            SELECT
              1
            FROM
              pg_sequences
            WHERE
              sequencename = '{table_after.pk_seq}'
              -- 2^63 - 1
              AND max_value = 9223372036854775807;
            """
        )
        assert cur.fetchone()


@pytest.mark.parametrize(
    "pk_type,convert_pk_to_bigint,expected_seq_max_val,expected_pk_type",
    (
        ("smallserial", False, 2147483647, "smallint"),
        ("smallserial", True, 9223372036854775807, "bigint"),
        ("serial", False, 2147483647, "int"),
        ("serial", True, 9223372036854775807, "bigint"),
        ("bigserial", False, 9223372036854775807, "bigint"),
    ),
)
def test_repack_full_with_serial_pk(
    connection: _psycopg.Connection,
    pk_type: str,
    convert_pk_to_bigint: bool,
    expected_seq_max_val: int,
    expected_pk_type: str,
) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type=pk_type,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
            convert_pk_to_bigint=convert_pk_to_bigint,
        )
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_when_table_has_large_value_being_inserted(
    connection: _psycopg.Connection,
) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type="bigint",
            referred_table_rows=10,
        )
        # Manipulate the underlying sequence so that the nextval is set to
        # a number that is greater than a signed 32bit integer.
        cur.execute(
            """
            ALTER SEQUENCE
              to_repack_seq
            INCREMENT BY
              9223372036854775706;
            """
        )
        # And insert into the table a new row that will have a large pk value.
        cur.execute(
            """
            INSERT INTO to_repack (
                var_with_btree,
                var_with_pattern_ops,
                int_with_check,
                int_with_not_valid_check,
                int_with_long_index_name,
                var_with_unique_idx,
                var_with_unique_const,
                var_with_deferrable_const,
                var_with_deferred_const,
                valid_fk,
                not_valid_fk,
                to_repack,
                var_maybe_with_exclusion,
                var_with_multiple_idx
            )
            SELECT
                substring(md5(random()::text), 1, 10),
                substring(md5(random()::text), 1, 10),
                (floor(random() * 10) + 1)::int,
                (floor(random() * 10) + 1)::int,
                (floor(random() * 10) + 1)::int,
                substring(md5(random()::text), 1, 10),
                substring(md5(random()::text), 1, 10),
                substring(md5(random()::text), 1, 10),
                substring(md5(random()::text), 1, 10),
                (floor(random() * 10) + 1)::int,
                (floor(random() * 10) + 1)::int,
                (floor(random() * 10) + 1)::int,
                substring(md5(random()::text), 1, 10),
                substring(md5(random()::text), 1, 10)
            FROM generate_series(1, 1);
        """
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            conn=connection,
            cur=cur,
            # Very large number so that we don't create a very large amount of
            # batches in the backfill log table.
            batch_size=9000000000000000000,
        )
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_when_table_does_not_have_pk_with_sequence(
    connection: _psycopg.Connection,
) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type="integer",
            ommit_sequence=True,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_when_post_backfill_batch_callback_is_passed(
    connection: _psycopg.Connection,
) -> None:
    batches = []

    def my_sweet_callback(batch: BackfillBatch) -> None:
        batches.append(batch)

    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=25,
            conn=connection,
            cur=cur,
            post_backfill_batch_callback=my_sweet_callback,
        )
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )

    # For a table of 100 rows with batch_size of 25, we have 4 batches:
    assert batches == [
        BackfillBatch(id=1, start=1, end=25),
        BackfillBatch(id=2, start=26, end=50),
        BackfillBatch(id=3, start=51, end=75),
        BackfillBatch(id=4, start=76, end=100),
    ]


def test_repeat_stage_when_lock_timeout(connection: _psycopg.Connection) -> None:
    with connection.cursor() as cur:
        if not _psycopg.PSYCOPG_3:  # pragma: no cover
            # https://github.com/psycopg/psycopg2/issues/941#issuecomment-864025101
            # https://github.com/psycopg/psycopg2/issues/1305#issuecomment-866712961
            # TODO: wrap the connection so that connection.cursor() doesn't
            # start in a transaction. Else, this test will fail.
            cur.execute("ABORT;")

        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )

        def side_effect(*args: object, **kwargs: object) -> None:
            repack.cur.execute("ABORT;")
            raise _psycopg.errors.LockNotAvailable(
                "canceling statement due to lock timeout"
            )

        # Pre-validation only uses introspection queries. There's no need for
        # lock timeouts there.
        repack.pre_validate()

        # Setting up the copy relations may time out (specially the trigger).
        with mock.patch.object(repack.command, "create_copy_trigger") as mocked:
            mocked.side_effect = side_effect
            with pytest.raises(FailureDueToLockTimeout):
                repack.setup_repacking()

        # It can be called again successfully as the function is idempotent and
        # reentrant.
        repack.setup_repacking()

        # Backfill only performs reads and writes. There's no need for lock
        # timeouts here.
        repack.backfill()

        # Syncing the schemas will involve firing multiple DDLs that may time
        # out.
        with mock.patch.object(
            repack.command, "create_unique_constraint_using_idx"
        ) as mocked:
            mocked.side_effect = side_effect
            with pytest.raises(FailureDueToLockTimeout):
                repack.sync_schemas()

        # It can be called again successfully as the function is idempotent and
        # reentrant.
        repack.sync_schemas()

        # Swapping also has many DDLs that may time out as it is moving the
        # tables around.
        with mock.patch.object(repack.command, "rename_table") as mocked:
            mocked.side_effect = side_effect
            with pytest.raises(FailureDueToLockTimeout):
                repack.swap()

        # It can be called again successfully as the function is idempotent and
        # reentrant (it also happens inside a transaction).
        repack.swap()

        # Reverting the swap is the same as the swap but with the opposite
        # relations.
        with mock.patch.object(repack.command, "rename_table") as mocked:
            mocked.side_effect = side_effect
            with pytest.raises(_psycopg.errors.LockNotAvailable):
                repack.revert_swap()

        # It can be called again successfully as the function is idempotent and
        # reentrant (it also happens inside a transaction).
        repack.revert_swap()

        # Swap again so that we can proceed to the next stage.
        repack.swap()

        # The clean-up function also produces DDLs when renaming idx and
        # constraints and those DDLs can time out.
        with mock.patch.object(repack.command, "rename_index") as mocked:
            mocked.side_effect = side_effect
            with pytest.raises(FailureDueToLockTimeout):
                repack.clean_up()

        # It can be called again successfully as the function is idempotent and
        # reentrant (it also happens inside a transaction).
        repack.clean_up()

        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_reset(connection: _psycopg.Connection) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        # Psycopack hasn't run yet, no reason to reset.
        with pytest.raises(InvalidStageForReset, match="Psycopack hasn't run yet"):
            repack.reset()

        # Resetting after pre-validate
        repack.pre_validate()
        repack.reset()
        _assert_reset(repack, cur)

        # Resetting after setup
        repack.pre_validate()
        repack.setup_repacking()
        repack.reset()
        _assert_reset(repack, cur)

        # Resetting after backfill
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.reset()
        _assert_reset(repack, cur)

        # Resetting after sync_schema
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        repack.reset()
        _assert_reset(repack, cur)

        # Resetting after swap results in error
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        repack.swap()
        with pytest.raises(InvalidStageForReset, match="reset from the CLEAN_UP stage"):
            repack.reset()

        # But if the swap is reverted, all should be good.
        repack.revert_swap()
        repack.reset()
        _assert_reset(repack, cur)

        # Run everything from scratch to check no malfunction has arised from
        # any reset.
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        repack.swap()
        repack.clean_up()

        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_when_invalid_indexes(connection: _psycopg.Connection) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        cur.execute("""
            UPDATE
              pg_index
            SET
              indisvalid = false
            WHERE
              indexrelid = 'btree_idx'::regclass;
        """)

        with pytest.raises(InvalidIndexes, match="btree_idx"):
            repack.full()


def test_with_non_default_schema(connection: _psycopg.Connection) -> None:
    schema = "sweet_schema"
    with connection.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema};")
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type="integer",
            schema=schema,
        )
        table_before = _collect_table_info(
            table="to_repack",
            connection=connection,
            schema=schema,
        )
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
            schema=schema,
        )
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        repack.swap()
        repack.revert_swap()
        repack.swap()
        repack.full()

        table_after = _collect_table_info(
            table="to_repack",
            connection=connection,
            schema=schema,
        )
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


def test_with_fks_from_another_schema(connection: _psycopg.Connection) -> None:
    with connection.cursor() as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=10,
            pk_type="integer",
        )
        repack = Repack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        schema = "sweet_schema"
        cur.execute(f"CREATE SCHEMA {schema};")
        cur.execute(
            dedent(f"""
            CREATE TABLE {schema}.referring_table (
              to_repack_id INTEGER REFERENCES public.to_repack(id)
            );
            """)
        )
        with pytest.raises(
            ReferringForeignKeyInDifferentSchema, match=f"{schema}.referring_table"
        ):
            repack.full()


def test_without_schema_privileges(connection: _psycopg.Connection) -> None:
    schema = "sweet_schema"
    with connection.cursor() as cur:
        cur.execute("SELECT current_user;")
        result = cur.fetchone()
        assert result is not None
        original_user = result[0]

        cur.execute(f"CREATE SCHEMA {schema};")
        cur.execute(f"REVOKE CREATE ON SCHEMA {schema} FROM PUBLIC;")
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=10,
            pk_type="integer",
            schema=schema,
        )
        cur.execute("DROP USER IF EXISTS sweet_user;")
        cur.execute("CREATE USER sweet_user;")
        # No CREATE nor USAGE privilege by default.
        cur.execute("SET ROLE sweet_user;")
        with pytest.raises(
            NoCreateAndUsagePrivilegeOnSchema,
            match="GRANT CREATE, USAGE ON SCHEMA sweet_schema TO sweet_user;",
        ):
            Repack(
                table="to_repack",
                batch_size=1,
                conn=connection,
                cur=cur,
                schema=schema,
            )
        cur.execute(f"SET ROLE '{original_user}';")
        # CREATE by itself is not enough; need USAGE too.
        cur.execute("GRANT CREATE ON SCHEMA sweet_schema TO sweet_user;")
        cur.execute("SET ROLE sweet_user;")
        with pytest.raises(
            NoCreateAndUsagePrivilegeOnSchema,
            match="GRANT CREATE, USAGE ON SCHEMA sweet_schema TO sweet_user;",
        ):
            Repack(
                table="to_repack",
                batch_size=1,
                conn=connection,
                cur=cur,
                schema=schema,
            )
        cur.execute(f"SET ROLE '{original_user}';")
        cur.execute("REVOKE CREATE ON SCHEMA sweet_schema FROM sweet_user;")
        # USAGE by itself is not enough; need CREATE too.
        cur.execute("GRANT USAGE ON SCHEMA sweet_schema TO sweet_user;")
        cur.execute("SET ROLE sweet_user;")
        with pytest.raises(
            NoCreateAndUsagePrivilegeOnSchema,
            match="GRANT CREATE, USAGE ON SCHEMA sweet_schema TO sweet_user;",
        ):
            Repack(
                table="to_repack",
                batch_size=1,
                conn=connection,
                cur=cur,
                schema=schema,
            )


def test_user_without_table_ownership(
    connection: _psycopg.Connection,
) -> None:
    schema = "sweet_schema"
    with connection.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema};")
        cur.execute(f"REVOKE CREATE ON SCHEMA {schema} FROM PUBLIC;")
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=10,
            schema=schema,
        )
        cur.execute("DROP USER IF EXISTS sweet_user;")
        cur.execute("CREATE USER sweet_user;")
        cur.execute("GRANT CREATE, USAGE ON SCHEMA sweet_schema TO sweet_user;")
        cur.execute("SET ROLE sweet_user;")
        with pytest.raises(
            NotTableOwner,
            match="ALTER TABLE sweet_schema.to_repack OWNER TO sweet_user;",
        ):
            Repack(
                table="to_repack",
                batch_size=1,
                conn=connection,
                cur=cur,
                schema=schema,
            )
