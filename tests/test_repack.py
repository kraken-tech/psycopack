import dataclasses

import pytest

from psycopack import (
    InheritedTable,
    Repack,
    TableDoesNotExist,
    TableHasTriggers,
    TableIsEmpty,
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


def _collect_table_info(table: str, connection: _psycopg.Connection) -> _TableInfo:
    with connection.cursor() as cur:
        introspector = _introspect.Introspector(
            conn=connection, cur=_cur.LoggedCursor(cur=cur)
        )
        oid = introspector.get_table_oid(table=table)
        assert oid is not None
        indexes = introspector.get_index_def(table=table)
        referring_fks = introspector.get_referring_fks(table=table)
        constraints = introspector.get_constraints(
            table=table, types=["c", "f", "n", "p", "u", "t", "x"]
        )

    return _TableInfo(
        oid=oid,
        indexes=indexes,
        referring_fks=referring_fks,
        constraints=constraints,
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

    # All functions and triggers are removed.
    trigger_info = _get_trigger_info(repack, cur)
    assert trigger_info.trigger_exists is False
    assert trigger_info.repacked_trigger_exists is False
    function_info = _get_function_info(repack, cur)
    assert function_info.function_exists is False
    assert function_info.repacked_function_exists is False

    # The tracker table itself will also be deleted after the clean up process.
    assert repack.introspector.get_table_oid(table=repack.tracker.tracker_table) is None


def test_repack_full(connection: _psycopg.Connection) -> None:
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
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


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


def test_revert_swap_after_swap_called(connection: _psycopg.Connection) -> None:
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
