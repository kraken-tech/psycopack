import dataclasses
from textwrap import dedent
from typing import Tuple, Union
from unittest import mock

import pytest

from psycopack import (
    BackfillBatch,
    CompositePrimaryKey,
    DeferrableUniqueConstraint,
    FailureDueToLockTimeout,
    InheritedTable,
    InvalidIndexes,
    InvalidPrimaryKeyTypeForConversion,
    InvalidStageForReset,
    NoCreateAndUsagePrivilegeOnSchema,
    NoReferencesPrivilege,
    NoReferringTableOwnership,
    NotTableOwner,
    PartitioningForTableWithReferringFKs,
    PrimaryKeyNotFound,
    Psycopack,
    ReferringForeignKeyInDifferentSchema,
    SyncStrategy,
    TableDoesNotExist,
    TableHasTriggers,
    TableIsEmpty,
    UnexpectedSyncStrategy,
    UnsupportedPrimaryKey,
    _const,
    _cur,
    _introspect,
    _partition,
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
    pk_seq_val: int | None


def _collect_table_info(
    table: str,
    connection: _psycopg.Connection,
    schema: str = "public",
) -> _TableInfo:
    with _cur.get_cursor(connection, logged=True) as cur:
        introspector = _introspect.Introspector(
            conn=connection,
            cur=cur,
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
        if pk_seq:
            pk_seq_val = introspector.get_pk_sequence_value(seq=pk_seq)
        else:
            pk_seq_val = None

    return _TableInfo(
        oid=oid,
        indexes=indexes,
        referring_fks=referring_fks,
        constraints=constraints,
        pk_seq=pk_seq,
        pk_seq_val=pk_seq_val,
    )


@dataclasses.dataclass
class _TriggerInfo:
    trigger_exists: bool
    repacked_trigger_exists: bool
    change_log_trigger_exists: bool


def _get_trigger_info(repack: Psycopack, cur: _cur.Cursor) -> _TriggerInfo:
    cur.execute(f"SELECT 1 FROM pg_trigger WHERE tgname = '{repack.trigger}'")
    trigger_exists = cur.fetchone() is not None
    cur.execute(f"SELECT 1 FROM pg_trigger WHERE tgname = '{repack.repacked_trigger}'")
    repacked_trigger_exists = cur.fetchone() is not None
    if repack.change_log is not None:
        cur.execute(
            f"SELECT 1 FROM pg_trigger WHERE tgname = '{repack.change_log_trigger}'"
        )
        change_log_trigger_exists = cur.fetchone() is not None
    else:
        change_log_trigger_exists = False

    repacked_trigger_exists = cur.fetchone() is not None
    return _TriggerInfo(
        trigger_exists=trigger_exists,
        repacked_trigger_exists=repacked_trigger_exists,
        change_log_trigger_exists=change_log_trigger_exists,
    )


@dataclasses.dataclass
class _FunctionInfo:
    function_exists: bool
    repacked_function_exists: bool
    change_log_function_exists: bool


def _get_function_info(repack: Psycopack, cur: _cur.Cursor) -> _FunctionInfo:
    cur.execute(f"SELECT 1 FROM pg_proc WHERE proname = '{repack.function}'")
    function_exists = cur.fetchone() is not None
    cur.execute(f"SELECT 1 FROM pg_proc WHERE proname = '{repack.repacked_function}'")
    repacked_function_exists = cur.fetchone() is not None
    if repack.change_log_function is not None:
        cur.execute(
            f"SELECT 1 FROM pg_proc WHERE proname = '{repack.change_log_function}'"
        )
        change_log_function_exists = cur.fetchone() is not None
    else:
        change_log_function_exists = False
    return _FunctionInfo(
        function_exists=function_exists,
        repacked_function_exists=repacked_function_exists,
        change_log_function_exists=change_log_function_exists,
    )


@dataclasses.dataclass
class _SequenceInfo:
    sequence_exists: bool


def _get_sequence_info(repack: Psycopack, cur: _cur.Cursor) -> _SequenceInfo:
    cur.execute(f"SELECT 1 FROM pg_sequences WHERE sequencename = '{repack.id_seq}'")
    sequence_exists = cur.fetchone() is not None
    return _SequenceInfo(sequence_exists=sequence_exists)


def _assert_repack(
    table_before: _TableInfo,
    table_after: _TableInfo,
    repack: Psycopack,
    cur: _cur.Cursor,
) -> None:
    # They aren't the same tables (thus different oids), but everything else is
    # the same.
    assert table_before.oid != table_after.oid
    assert table_before.indexes == table_after.indexes
    assert table_before.referring_fks == table_after.referring_fks
    assert table_before.constraints == table_after.constraints
    assert table_before.pk_seq == table_after.pk_seq
    if table_before.pk_seq_val is None or table_before.pk_seq_val > 0:
        assert table_before.pk_seq_val == table_after.pk_seq_val
    else:
        assert table_after.pk_seq_val is None or table_after.pk_seq_val >= 2**31

    # All functions and triggers are removed.
    trigger_info = _get_trigger_info(repack, cur)
    assert trigger_info.trigger_exists is False
    assert trigger_info.repacked_trigger_exists is False
    function_info = _get_function_info(repack, cur)
    assert function_info.function_exists is False
    assert function_info.repacked_function_exists is False

    # The tracker table itself will also be deleted after the clean up process.
    assert repack.introspector.get_table_oid(table=repack.tracker.tracker_table) is None

    # The row in the Registry will also be removed after the process is done.
    cur.execute(
        f"SELECT 1 FROM {repack.schema}.{_const.PSYCOPACK_REGISTRY} "
        f"WHERE original_table = '{repack.table}';"
    )
    assert cur.fetchone() is None


def _assert_reset(repack: Psycopack, cur: _cur.Cursor) -> None:
    trigger_info = _get_trigger_info(repack, cur)
    assert trigger_info.trigger_exists is False

    function_info = _get_function_info(repack, cur)
    assert function_info.function_exists is False
    assert _get_sequence_info(repack, cur).sequence_exists is False
    assert repack.introspector.get_table_oid(table=repack.copy_table) is None
    assert repack.introspector.get_table_oid(table=repack.tracker.tracker_table) is None

    if repack.sync_strategy == SyncStrategy.CHANGE_LOG:
        assert trigger_info.change_log_trigger_exists is False
        assert function_info.change_log_function_exists is False
        assert repack.change_log is not None
        assert repack.introspector.get_table_oid(table=repack.change_log) is None


def _do_writes(
    table: str,
    cur: _cur.Cursor,
    schema: str = "public",
    check_table: str | None = None,
) -> None:
    """
    Do some writes (insert, update, delete) to check that the copy function works.
    """
    cur.execute(
        dedent(f"""
        INSERT INTO {schema}.{table} (
            var_with_btree,
            var_with_pattern_ops,
            int_with_check,
            int_with_not_valid_check,
            int_with_long_index_name,
            var_with_unique_idx,
            var_with_unique_const,
            valid_fk,
            not_valid_fk,
            {table},
            var_maybe_with_exclusion,
            var_with_multiple_idx
        )
        VALUES (
            substring(md5(random()::text), 1, 10),
            substring(md5(random()::text), 1, 10),
            (floor(random() * 10) + 1)::int,
            (floor(random() * 10) + 1)::int,
            (floor(random() * 10) + 1)::int,
            substring(md5(random()::text), 1, 10),
            substring(md5(random()::text), 1, 10),
            (floor(random() * 10) + 1)::int,
            (floor(random() * 10) + 1)::int,
            (floor(random() * 10) + 1)::int,
            substring(md5(random()::text), 1, 10),
            substring(md5(random()::text), 1, 10)
        )
        RETURNING id;
    """)
    )
    result = cur.fetchone()
    assert result is not None
    id_ = result[0]
    if check_table is not None:
        assert _query_row(table=table, id_=id_, cur=cur, schema=schema) == _query_row(
            table=check_table, id_=id_, cur=cur, schema=schema
        )

    cur.execute(f"UPDATE {schema}.{table} SET var_with_btree = 'foo' WHERE id = {id_};")
    if check_table is not None:
        assert _query_row(table=table, id_=id_, cur=cur, schema=schema) == _query_row(
            table=check_table, id_=id_, cur=cur, schema=schema
        )

    cur.execute(f"DELETE FROM {schema}.{table} WHERE id = {id_};")
    assert _query_row(table=table, id_=id_, cur=cur, schema=schema) is None
    if check_table is not None:
        assert _query_row(table=check_table, id_=id_, cur=cur, schema=schema) is None


def _query_row(
    table: str,
    id_: int,
    cur: _cur.Cursor,
    schema: str = "public",
) -> Tuple[Union[int, str], ...] | None:
    cur.execute(f"SELECT * FROM {schema}.{table} WHERE id = {id_};")
    return cur.fetchone()


@pytest.mark.parametrize(
    "pk_type",
    ("bigint", "bigserial", "integer", "serial", "smallint", "smallserial"),
)
def test_repack_full(connection: _psycopg.Connection, pk_type: str) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type=pk_type,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type=pk_type,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
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


def test_repack_full_with_identity_pk_with_psycopg_cursor(
    connection: _psycopg.Connection,
) -> None:
    pk_type = "integer generated by default as identity"
    identity_type = "d"
    with _cur.get_cursor(connection) as cur:  # logged=False is the default
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type=pk_type,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
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
        pk_info = repack.introspector.get_primary_key_info(table="to_repack")
        assert pk_info is not None
        assert pk_info.identity_type == identity_type


def test_when_table_does_not_exist(connection: _psycopg.Connection) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        with pytest.raises(TableDoesNotExist):
            Psycopack(
                table="to_repack",
                batch_size=1,
                conn=connection,
                cur=cur,
            ).full()


def test_when_table_is_empty(connection: _psycopg.Connection) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        cur.execute("CREATE TABLE empty_table (id SERIAL PRIMARY KEY);")

        with pytest.raises(TableIsEmpty):
            Psycopack(
                table="empty_table",
                batch_size=1,
                conn=connection,
                cur=cur,
            ).full()


def test_when_table_is_empty_with_allow_empty(
    connection: _psycopg.Connection,
) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        cur.execute("CREATE TABLE empty_table (id SERIAL PRIMARY KEY);")

        # doesn't raise TableIsEmpty
        Psycopack(
            table="empty_table",
            batch_size=1,
            conn=connection,
            cur=cur,
            allow_empty=True,
        ).full()


def test_repack_full_after_pre_validate_called(connection: _psycopg.Connection) -> None:
    """
    full() should be able to be called no matter where the repacking process
    left out from.

    In this case, it will just proceed from the pre_validation step.
    """
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        # Sync schemas finished. Next stage is post sync update.
        assert repack.tracker.get_current_stage() == _tracker.Stage.POST_SYNC_UPDATE
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        repack.post_sync_update()
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        repack.post_sync_update()
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )

        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        assert repack.tracker.get_current_stage() == _tracker.Stage.POST_SYNC_UPDATE

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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        repack = Psycopack(
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


def test_registry_table_sync_strategy_upgrade(connection: _psycopg.Connection) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        # Create the Registry table with the OLD schema manually.
        cur.execute(
            dedent(f"""
            CREATE TABLE public.{_const.PSYCOPACK_REGISTRY} (
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

        # Update is checked upon initialisation.
        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        columns = repack.introspector.get_table_columns(table=_const.PSYCOPACK_REGISTRY)
        assert "sync_strategy" in columns
        assert "change_log_trigger" in columns
        assert "change_log" in columns
        assert "change_log_function" in columns
        assert "change_log_copy_function" in columns


def test_when_user_changes_existing_sync_strategy(
    connection: _psycopg.Connection,
) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        # Initiate psycopack with 'DIRECT_TRIGGER' strategy - this creates the
        # Registry table.
        Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
            sync_strategy=SyncStrategy.DIRECT_TRIGGER,
        )
        with pytest.raises(UnexpectedSyncStrategy):
            Psycopack(
                table="to_repack",
                batch_size=1,
                conn=connection,
                cur=cur,
                sync_strategy=SyncStrategy.CHANGE_LOG,
            )


def test_table_to_repack_deleted_after_pre_validation(
    connection: _psycopg.Connection,
) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
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

        repack.post_sync_update()
        with pytest.raises(_tracker.StageAlreadyFinished):
            repack.post_sync_update()

        repack.swap()
        with pytest.raises(_tracker.StageAlreadyFinished):
            repack.swap()

        repack.clean_up()
        with pytest.raises(_tracker.InvalidPsycopackSetup):
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        with pytest.raises(_tracker.InvalidPsycopackSetup):
            # Can't initialise a repacking without going through pre-validation
            # first.
            repack.setup_repacking()

        repack.pre_validate()

        with pytest.raises(_tracker.InvalidPsycopackStep):
            # Can't go to backfill without setting up first.
            repack.backfill()

        repack.setup_repacking()

        with pytest.raises(_tracker.InvalidPsycopackStep):
            # Can't go to sync schemas without backfilling first
            repack.sync_schemas()

        repack.backfill()

        with pytest.raises(_tracker.InvalidPsycopackStep):
            # Can't go to swap without syncing schemas first.
            repack.swap()

        repack.sync_schemas()
        with pytest.raises(_tracker.InvalidPsycopackStep):
            # Can't go to swap without post sync update.
            repack.swap()

        repack.post_sync_update()
        with pytest.raises(_tracker.InvalidPsycopackStep):
            # Can't go to clean up without swap.
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type="integer",
            ommit_sequence=ommit_sequence,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        repack.post_sync_update()

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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            with_exclusion_constraint=True,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
        cur.execute("CREATE TABLE parent (id SERIAL PRIMARY KEY);")
        cur.execute("CREATE TABLE child () INHERITS (parent);")
        # Insert a row so that the table passes the TableIsEmpty check.
        cur.execute("INSERT INTO child (id) SELECT generate_series(1, 1);")
        repack = Psycopack(
            table="child",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        with pytest.raises(InheritedTable):
            repack.full()


def test_repack_when_table_has_triggers(connection: _psycopg.Connection) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
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
        repack = Psycopack(
            table="table_with_triggers",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        with pytest.raises(TableHasTriggers):
            repack.full()


def test_table_without_pk(connection: _psycopg.Connection) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        cur.execute("CREATE TABLE table_without_fk (id integer);")
        # Insert a row so that the table passes the TableIsEmpty check.
        cur.execute("INSERT INTO table_without_fk (id) VALUES (42)")
        repack = Psycopack(
            table="table_without_fk",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        with pytest.raises(PrimaryKeyNotFound):
            repack.full()


def test_table_without_supported_pk_type(connection: _psycopg.Connection) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        cur.execute("CREATE TABLE table_with_var_pk (id varchar PRIMARY KEY);")
        # Insert a row so that the table passes the TableIsEmpty check.
        cur.execute("INSERT INTO table_with_var_pk (id) VALUES ('gday')")
        repack = Psycopack(
            table="table_with_var_pk",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        with pytest.raises(UnsupportedPrimaryKey):
            repack.full()


def test_with_pk_name_different_than_id(connection: _psycopg.Connection) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_name="a_name_that_is_not_id",
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
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
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
        cur.execute(f"CREATE TABLE table_with_big_pk (id {pk_type} PRIMARY KEY);")
        # Insert a row so that the table passes the TableIsEmpty check.
        cur.execute("INSERT INTO table_with_big_pk (id) VALUES (42)")
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type=initial_pk_type,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type=pk_type,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
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


def test_when_table_has_negative_pk_values(
    connection: _psycopg.Connection,
) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type="integer",
            pk_start=-200,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
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


@pytest.mark.parametrize(
    "initial_pk_type",
    (
        "integer",
        "serial",
        "smallint",
        "smallserial",
    ),
)
def test_with_writes_when_table_has_negative_pk_values(
    connection: _psycopg.Connection, initial_pk_type: str
) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type=initial_pk_type,
            pk_start=-200,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)

        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
            convert_pk_to_bigint=True,
        )
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        _do_writes(table="to_repack", cur=cur, check_table=repack.copy_table)
        repack.sync_schemas()
        _do_writes(table="to_repack", cur=cur, check_table=repack.copy_table)
        repack.post_sync_update()
        _do_writes(table="to_repack", cur=cur, check_table=repack.copy_table)
        repack.swap()
        _do_writes(table="to_repack", cur=cur, check_table=repack.repacked_name)
        repack.clean_up()
        _do_writes(table="to_repack", cur=cur)

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
    with _cur.get_cursor(connection, logged=True) as cur:
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
                (floor(random() * 10) + 1)::int,
                (floor(random() * 10) + 1)::int,
                (floor(random() * 10) + 1)::int,
                substring(md5(random()::text), 1, 10),
                substring(md5(random()::text), 1, 10)
            FROM generate_series(1, 1);
        """
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type="integer",
            ommit_sequence=True,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
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

    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
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
        repack = Psycopack(
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
        with mock.patch.object(
            repack.command, "create_source_to_copy_trigger"
        ) as mocked:
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

        # It can be called again successfully as the function is idempotent and
        # reentrant.
        repack.post_sync_update()

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


@pytest.mark.parametrize(
    "sync_strategy",
    [SyncStrategy.DIRECT_TRIGGER, SyncStrategy.CHANGE_LOG],
)
def test_reset(connection: _psycopg.Connection, sync_strategy: SyncStrategy) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)

        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
            sync_strategy=sync_strategy,
            change_log_batch_size=10
            if sync_strategy == SyncStrategy.CHANGE_LOG
            else None,
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

        # Resetting after post_sync_update
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        repack.post_sync_update()
        repack.reset()
        _assert_reset(repack, cur)

        # Resetting after swap results in error
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        repack.post_sync_update()
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
        repack.post_sync_update()
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        repack = Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
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
        repack = Psycopack(
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
        repack.post_sync_update()
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
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=10,
            pk_type="integer",
        )
        repack = Psycopack(
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


def test_with_deferrable_unique_constraint(connection: _psycopg.Connection) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        cur.execute(
            "ALTER TABLE to_repack DROP CONSTRAINT to_repack_var_with_unique_const_key;"
        )
        cur.execute(
            dedent("""
                ALTER TABLE to_repack ADD CONSTRAINT to_repack_var_with_unique_const_key
                UNIQUE (var_with_unique_const)
                DEFERRABLE;
            """)
        )

        with pytest.raises(
            DeferrableUniqueConstraint, match="to_repack_var_with_unique_const_key"
        ):
            repack.full()


def test_partition_with_referring_fks(connection: _psycopg.Connection) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type="integer",
        )
        # Create a referring table with FK to to_repack
        cur.execute("DROP TABLE IF EXISTS referring_table;")
        cur.execute(
            dedent("""
            CREATE TABLE referring_table (
              id SERIAL PRIMARY KEY,
              to_repack_id INTEGER REFERENCES to_repack(id)
            );
            """)
        )
        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
            partition_config=_partition.PartitionConfig(
                column="datetime_field",
                num_of_extra_partitions_ahead=10,
                strategy=_partition.DateRangeStrategy(
                    partition_by=_partition.PartitionInterval.DAY
                ),
            ),
        )

        with pytest.raises(
            PartitioningForTableWithReferringFKs,
            match="referring foreign keys",
        ):
            repack.full()


def test_without_schema_privileges(connection: _psycopg.Connection) -> None:
    schema = "sweet_schema"
    with _cur.get_cursor(connection, logged=True) as cur:
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
            Psycopack(
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
            Psycopack(
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
            Psycopack(
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
    with _cur.get_cursor(connection, logged=True) as cur:
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
            Psycopack(
                table="to_repack",
                batch_size=1,
                conn=connection,
                cur=cur,
                schema=schema,
            )


def test_user_without_referring_table_ownership(
    connection: _psycopg.Connection,
) -> None:
    schema = "sweet_schema"
    with _cur.get_cursor(connection, logged=True) as cur:
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
        cur.execute("ALTER TABLE sweet_schema.to_repack OWNER TO sweet_user;")
        cur.execute("SET ROLE sweet_user;")
        with pytest.raises(
            NoReferringTableOwnership,
            match="ALTER TABLE sweet_schema.referring_table OWNER TO sweet_user;",
        ):
            Psycopack(
                table="to_repack",
                batch_size=1,
                conn=connection,
                cur=cur,
                schema=schema,
            )


def test_user_without_referred_table_references_privilege(
    connection: _psycopg.Connection,
) -> None:
    schema = "sweet_schema"
    with _cur.get_cursor(connection, logged=True) as cur:
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
        cur.execute("ALTER TABLE sweet_schema.to_repack OWNER TO sweet_user;")
        cur.execute("ALTER TABLE sweet_schema.referring_table OWNER TO sweet_user;")
        cur.execute(
            "ALTER TABLE sweet_schema.not_valid_referring_table OWNER TO sweet_user;"
        )
        cur.execute("SET ROLE sweet_user;")
        with pytest.raises(
            NoReferencesPrivilege,
            match="GRANT REFERENCES ON TABLE sweet_schema.referred_table TO sweet_user;",
        ):
            Psycopack(
                table="to_repack",
                batch_size=1,
                conn=connection,
                cur=cur,
                schema=schema,
            )


def test_user_with_bare_minimum_permissions(connection: _psycopg.Connection) -> None:
    schema = "sweet_schema"
    with _cur.get_cursor(connection, logged=True) as cur:
        cur.execute(f"CREATE SCHEMA {schema};")
        cur.execute(f"REVOKE CREATE ON SCHEMA {schema} FROM PUBLIC;")
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=10,
            schema=schema,
        )
        table_before = _collect_table_info(
            table="to_repack",
            connection=connection,
            schema=schema,
        )
        cur.execute("DROP USER IF EXISTS sweet_user;")
        cur.execute("CREATE USER sweet_user;")
        cur.execute("GRANT CREATE, USAGE ON SCHEMA sweet_schema TO sweet_user;")
        cur.execute("ALTER TABLE sweet_schema.to_repack OWNER TO sweet_user;")
        cur.execute("ALTER TABLE sweet_schema.referring_table OWNER TO sweet_user;")
        cur.execute(
            "ALTER TABLE sweet_schema.not_valid_referring_table OWNER TO sweet_user;"
        )
        cur.execute(
            "GRANT REFERENCES ON TABLE sweet_schema.referred_table TO sweet_user;"
        )
        cur.execute(
            "GRANT REFERENCES ON TABLE sweet_schema.not_valid_referred_table TO sweet_user;"
        )
        cur.execute("SET ROLE sweet_user;")
        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
            schema=schema,
        )
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


def test_user_with_permissions_via_role(connection: _psycopg.Connection) -> None:
    schema = "sweet_schema"
    with _cur.get_cursor(connection, logged=True) as cur:
        cur.execute(f"CREATE SCHEMA {schema};")
        cur.execute(f"REVOKE CREATE ON SCHEMA {schema} FROM PUBLIC;")
        cur.execute("DROP ROLE IF EXISTS owner_role;")
        cur.execute("CREATE ROLE owner_role CREATEROLE;")
        cur.execute("GRANT CREATE, USAGE ON SCHEMA sweet_schema TO owner_role;")
        cur.execute("DROP USER IF EXISTS sweet_user;")
        cur.execute("CREATE USER sweet_user;")
        cur.execute("GRANT owner_role TO sweet_user;")
        cur.execute("SET ROLE owner_role;")
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=10,
            schema=schema,
        )
        table_before = _collect_table_info(
            table="to_repack",
            connection=connection,
            schema=schema,
        )
        cur.execute("SET ROLE sweet_user;")
        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
            schema=schema,
        )
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


def test_when_repack_is_reinstantiated_after_swapping(
    connection: _psycopg.Connection,
) -> None:
    """
    This test covers the following bug case:

    - The user runs the swap() command.
    - Now the copy table has been swapped with the original.
    - Their script crashes and they have to re-instantiate the Psycopack class.
    - The new Psycopack instance thinks that the recently-swapped class is the
      original.
    - The instance can't find any existing Psycopack objects, because they are
      all named after the OID of the original table.
    - Psycopack can't finish the process because it thinks the process never
      started.

    The Psycopack process should pick up where it left, no matter if the user
    has re-instantiated the class after the swap or not.
    """
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
        )
        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        repack.post_sync_update()
        repack.swap()

        # Re-instantiate to trigger the edge-case
        new_repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
        )
        # Pick up from clean-up
        new_repack.clean_up()

        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


@pytest.mark.parametrize(
    "positive_rows, negative_rows, expected_ranges",
    [
        (100, 0, [(1, 50), (51, 100)]),
        (0, 100, [(-1000, -951), (-950, -901)]),
        (100, 100, [(1, 50), (51, 100), (-1000, -951), (-950, -901)]),
    ],
)
def test_populate_backfill_log(
    connection: _psycopg.Connection,
    positive_rows: int,
    negative_rows: int,
    expected_ranges: list[tuple[int, int]],
) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_backfilling(
            cur=cur,
            positive_rows=positive_rows,
            negative_rows=negative_rows,
        )
        repack = Psycopack(
            table="to_backfill",
            batch_size=50,
            conn=connection,
            cur=cur,
        )
        repack.pre_validate()

        repack.setup_repacking()  # calls self._populate_backfill_log()

        cur.execute(
            f"SELECT batch_start, batch_end FROM {repack.backfill_log} ORDER BY id;"
        )
        result = cur.fetchall()
        assert result == expected_ranges


def test_with_skip_permissions_check(
    connection: _psycopg.Connection,
) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )

        with mock.patch.object(
            Psycopack, "_check_user_permissions", autospec=True
        ) as mocked:
            Psycopack(
                table="to_repack",
                batch_size=1,
                conn=connection,
                cur=cur,
                skip_permissions_check=True,
            )
            mocked.assert_not_called()


@pytest.mark.parametrize(
    "pk_type",
    ("bigint", "bigserial", "integer", "serial", "smallint", "smallserial"),
)
def test_repack_with_change_log_strategy(
    connection: _psycopg.Connection, pk_type: str
) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type=pk_type,
        )
        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
            sync_strategy=SyncStrategy.CHANGE_LOG,
            change_log_batch_size=10,
        )
        repack.pre_validate()
        repack.setup_repacking()

        create_row_sql = dedent(
            """
            INSERT INTO to_repack (
                var_with_btree,
                var_with_pattern_ops,
                int_with_check,
                int_with_not_valid_check,
                int_with_long_index_name,
                var_with_unique_idx,
                var_with_unique_const,
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
                (floor(random() * 10) + 1)::int,
                (floor(random() * 10) + 1)::int,
                (floor(random() * 10) + 1)::int,
                substring(md5(random()::text), 1, 10),
                substring(md5(random()::text), 1, 10)
            FROM
              generate_series(1, 1)
            RETURNING
              id
            ;
        """
        )
        # Insert a row in the table to_repack, to verify the trigger places the
        # pk of the row being changed in the change log.
        cur.execute(create_row_sql)

        cur.execute(f"SELECT * FROM {repack.change_log};")
        # The fixture adds 100 rows, and so the insert above was 101.
        assert cur.fetchall() == [(1, 101)]

        # Updating the same row doesn't create a new row in the change log
        # and doesn't err due to the "ON CONFLICT DO NOTHING".
        cur.execute("UPDATE to_repack SET int_with_check = 42 WHERE id = 101;")
        cur.execute(f"SELECT * FROM {repack.change_log};")
        assert cur.fetchall() == [(1, 101)]

        # Unless... It's updating the id itself.
        # The older id still remains in the table, however. Given that the copy
        # function is idempotent, this doesn't matter. But it additionally
        # covers for the corner case where the id is inserted again.
        cur.execute("UPDATE to_repack SET id = 9999 WHERE id = 101;")
        cur.execute(f"SELECT * FROM {repack.change_log};")
        assert cur.fetchall() == [(1, 101), (3, 9999)]

        # Deleting the row doesn't create a new row in the change log.
        cur.execute("DELETE FROM to_repack WHERE id = 9999;")
        cur.execute(f"SELECT * FROM {repack.change_log};")
        assert cur.fetchall() == [(1, 101), (3, 9999)]
        repack.backfill()

        # Create a row post-backfill, this row shouldn't be in the copy table
        # yet as it didn't exist prior to the backfill process.
        cur.execute(create_row_sql)
        row = cur.fetchone()
        assert row is not None
        created_row_id = row[0]
        assert created_row_id == 102
        cur.execute(
            f"SELECT count(*) FROM {repack.copy_table} WHERE id = {created_row_id};"
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 0

        # But it is in the change log.
        cur.execute(f"SELECT * FROM {repack.change_log};")
        assert cur.fetchall() == [(1, 101), (3, 9999), (5, 102)]

        # Before syncing the schema, the src-to-copy trigger doesn't exist.
        # But the change log trigger and function exist.
        cur.execute(f"SELECT 1 FROM pg_trigger WHERE tgname = '{repack.trigger}'")
        assert cur.fetchone() is None
        cur.execute(
            f"SELECT 1 FROM pg_proc WHERE proname = '{repack.change_log_function}'"
        )
        assert cur.fetchone() is not None
        cur.execute(
            f"SELECT 1 FROM pg_trigger WHERE tgname = '{repack.change_log_trigger}'"
        )
        assert cur.fetchone() is not None

        repack.sync_schemas()

        # After syncing the schema, the src-to-copy trigger exists.
        # But the change log trigger and function don't exist.
        cur.execute(f"SELECT 1 FROM pg_trigger WHERE tgname = '{repack.trigger}'")
        assert cur.fetchone() is not None
        cur.execute(
            f"SELECT 1 FROM pg_proc WHERE proname = '{repack.change_log_function}'"
        )
        assert cur.fetchone() is None
        cur.execute(
            f"SELECT 1 FROM pg_trigger WHERE tgname = '{repack.change_log_trigger}'"
        )
        assert cur.fetchone() is None

        # Before the post sync-update, there must be three rows in the change
        # log (id 101, 102, 9999).
        cur.execute(f"SELECT COUNT(*) FROM {repack.change_log};")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 3

        repack.post_sync_update()

        # Change log rows have been deleted.
        cur.execute(f"SELECT COUNT(*) FROM {repack.change_log};")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 0

        table_before = _collect_table_info(table="to_repack", connection=connection)
        repack.swap()

        # Before the clean-up, the change_log table is still there.
        assert repack.change_log is not None
        repack.introspector.get_table_oid(table=repack.change_log)

        repack.clean_up()

        # After the clean-up, the change_log table has been deleted.
        repack.introspector.get_table_oid(table=repack.change_log)

        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )


@pytest.mark.parametrize(
    "sync_strategy",
    [SyncStrategy.DIRECT_TRIGGER, SyncStrategy.CHANGE_LOG],
)
def test_multiple_foreign_keys_from_same_referring_table(
    connection: _psycopg.Connection,
    sync_strategy: SyncStrategy,
) -> None:
    """
    Test that multiple foreign keys from the same referring table are correctly
    handled during clean_up(). This verifies the fix for the bug where only one
    FK would be renamed correctly when multiple FKs from the same table
    existed.
    """
    with _cur.get_cursor(connection, logged=True) as cur:
        # Create the table to be repacked (simulating a users table)
        cur.execute(
            dedent("""
            CREATE TABLE person (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100)
            );
            """)
        )
        cur.execute("INSERT INTO person (name) VALUES ('Anna'), ('Bob'), ('Carlos');")

        # Create a referring table with TWO foreign keys to the same table
        # (simulating created_by and updated_by columns)
        cur.execute(
            dedent("""
            CREATE TABLE posts (
                id SERIAL PRIMARY KEY,
                title VARCHAR(200),
                created_by_id INTEGER REFERENCES person(id),
                updated_by_id INTEGER REFERENCES person(id)
            );
            """)
        )
        cur.execute(
            dedent("""
            INSERT INTO posts (title, created_by_id, updated_by_id)
            VALUES
                ('Post 1', 1, 1),
                ('Post 2', 1, 2),
                ('Post 3', 2, 3);
            """)
        )

        # Collect FK info before repacking
        introspector = _introspect.Introspector(
            conn=connection,
            cur=cur,
            schema="public",
        )
        fks_before = introspector.get_referring_fks(table="person")
        assert len(fks_before) == 2, "Expected 2 foreign keys from posts table"
        fk_names_before = {fk.name for fk in fks_before}

        # Run the full repack
        repack = Psycopack(
            table="person",
            batch_size=10,
            conn=connection,
            cur=cur,
            sync_strategy=sync_strategy,
            change_log_batch_size=100,
        )
        repack.full()

        # Verify both foreign keys still exist and point to the person table
        fks_after = introspector.get_referring_fks(table="person")
        assert len(fks_after) == 2, "Expected 2 foreign keys after repack"
        fk_names_after = {fk.name for fk in fks_after}

        # FK names should be preserved
        assert fk_names_before == fk_names_after, (
            f"FK names changed: before={fk_names_before}, after={fk_names_after}"
        )

        # Verify both FKs are still valid by checking constraints
        cur.execute(
            dedent("""
            SELECT
              conname,
              conrelid::regclass,
              confrelid::regclass
            FROM
              pg_constraint
            WHERE
              confrelid = 'person'::regclass
              AND contype = 'f'
            ORDER BY
              conname;
            """)
        )
        constraints = cur.fetchall()
        assert len(constraints) == 2

        # Verify both FKs point to the person table (not the old repacked table)
        for constraint in constraints:
            constraint_name, referring_table, referenced_table = constraint
            assert str(referenced_table) == "person"
            assert str(referring_table) == "posts"

        # Verify data integrity: FK constraints should still work
        cur.execute("SELECT COUNT(*) FROM posts;")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 3

        # Verify we can still query using the foreign keys
        cur.execute(
            dedent("""
            SELECT
              p.title,
              u1.name as creator,
              u2.name as updater
            FROM posts p
            JOIN person u1
              ON p.created_by_id = u1.id
            JOIN person u2
              ON p.updated_by_id = u2.id
            ORDER BY p.id;
            """)
        )
        rows = cur.fetchall()
        assert len(rows) == 3
        assert rows[0] == ("Post 1", "Anna", "Anna")
        assert rows[1] == ("Post 2", "Anna", "Bob")
        assert rows[2] == ("Post 3", "Bob", "Carlos")


def test_repack_with_day_range_partition(connection: _psycopg.Connection) -> None:
    with _cur.get_cursor(connection) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        # Clean up any referring tables from other tests
        cur.execute("DROP TABLE IF EXISTS referring_table CASCADE;")
        cur.execute("DROP TABLE IF EXISTS not_valid_referring_table CASCADE;")
        cur.execute("DROP TABLE IF EXISTS other_referring_table CASCADE;")
        # Insert a row with a datetime field for the last day of January to
        # guarantee all of January will be covered by partitions.
        create_row_sql = dedent(
            """
            INSERT INTO
              to_repack
              (datetime_field)
            VALUES
              ('2025-01-31 23:59:59');
        """
        )
        # Insert a row in the table to_repack, to verify the trigger places the
        # pk of the row being changed in the change log.
        cur.execute(create_row_sql)
        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
            partition_config=_partition.PartitionConfig(
                column="datetime_field",
                num_of_extra_partitions_ahead=10,
                strategy=_partition.DateRangeStrategy(
                    partition_by=_partition.PartitionInterval.DAY
                ),
            ),
        )
        repack.full()

        # Verify the table is now partitioned
        cur.execute("""
            SELECT
              pt.relname AS partition_name
            FROM
              pg_inherits
            JOIN
              pg_class pt
              ON pt.oid = inhrelid
            JOIN
              pg_class p
              ON p.oid = inhparent
            WHERE
              p.relname = 'to_repack'
            ORDER BY
              partition_name;
        """)
        partitions = cur.fetchall()
        assert len(partitions) > 0, "Table should have partitions"

        # Verify partition names follow the date-based format
        # Example: to_repack_p20250106 (table_name_pYYYYMMDD for DAY)
        # We should have partitions from MIN(datetime_field) to MAX(datetime_field) + 10 extra days
        # Since we inserted a row with 2025-01-31, we know that will be included
        partition_names = [p[0] for p in partitions]

        # All partitions should follow the naming format: to_repack_pYYYYMMDD
        for partition_name in partition_names:
            assert partition_name.startswith("to_repack_p"), (
                f"Invalid partition name: {partition_name}"
            )
            date_part = partition_name.replace("to_repack_p", "")
            assert len(date_part) == 8, (
                f"Date part should be 8 digits (YYYYMMDD): {partition_name}"
            )
            assert date_part.isdigit(), f"Date part should be numeric: {partition_name}"

        # Verify we have the partition for the explicitly inserted row (2025-01-31)
        assert "to_repack_p20250131" in partition_names, (
            "Should have partition for 2025-01-31"
        )

        # Verify we have 10 extra partitions after the max date (into February)
        february_partitions = [
            p for p in partition_names if p.startswith("to_repack_p202502")
        ]
        assert len(february_partitions) == 10, (
            f"Should have exactly 10 partitions in February, got {len(february_partitions)}"
        )

        # Verify the primary key includes both id and datetime_field (partition column)
        cur.execute(
            """
            SELECT
              a.attname AS column_name
            FROM
              pg_index i
            JOIN
              pg_attribute a
              ON a.attrelid = i.indrelid
              AND a.attnum = ANY(i.indkey)
            WHERE
              i.indrelid = 'to_repack'::regclass
              AND i.indisprimary
            ORDER BY
              array_position(i.indkey, a.attnum);
        """
        )
        pk_columns = [row[0] for row in cur.fetchall()]
        assert pk_columns == ["id", "datetime_field"]

        # Verify data integrity
        cur.execute("SELECT COUNT(*) FROM to_repack;")
        count = cur.fetchone()
        assert count is not None
        assert count[0] == 101, "All rows should be present (100 + 1 inserted)"


def test_repack_with_month_range_partition(connection: _psycopg.Connection) -> None:
    with _cur.get_cursor(connection) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        # Clean up any referring tables from other tests
        cur.execute("DROP TABLE IF EXISTS referring_table CASCADE;")
        cur.execute("DROP TABLE IF EXISTS not_valid_referring_table CASCADE;")
        cur.execute("DROP TABLE IF EXISTS other_referring_table CASCADE;")
        # Insert a row with a datetime field for the last day of June to
        # guarantee partitions from Jan to Jun (plus extras) will be created.
        create_row_sql = dedent(
            """
            INSERT INTO
              to_repack
              (datetime_field)
            VALUES
              ('2025-06-30 23:59:59');
        """
        )
        cur.execute(create_row_sql)
        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
            partition_config=_partition.PartitionConfig(
                column="datetime_field",
                num_of_extra_partitions_ahead=3,
                strategy=_partition.DateRangeStrategy(
                    partition_by=_partition.PartitionInterval.MONTH
                ),
            ),
        )
        repack.full()

        # Verify the table is now partitioned
        cur.execute("""
            SELECT
              pt.relname AS partition_name
            FROM
              pg_inherits
            JOIN
              pg_class pt
              ON pt.oid = inhrelid
            JOIN
              pg_class p
              ON p.oid = inhparent
            WHERE
              p.relname = 'to_repack'
            ORDER BY
              partition_name;
        """)
        partitions = cur.fetchall()
        assert len(partitions) > 0, "Table should have partitions"

        # Verify partition names follow the date-based format
        # Example: to_repack_p202501 (table_name_pYYYYMM for MONTH)
        # We should have partitions from MIN(datetime_field) to MAX(datetime_field) + 3 extra months
        # Since we inserted a row with 2025-06-30, we know June will be included
        partition_names = [p[0] for p in partitions]
        assert partition_names == [
            "to_repack_p202501",
            "to_repack_p202502",
            "to_repack_p202503",
            "to_repack_p202504",
            "to_repack_p202505",
            "to_repack_p202506",
            "to_repack_p202507",
            "to_repack_p202508",
            "to_repack_p202509",
        ]

        # Verify the primary key includes both id and datetime_field (partition column)
        cur.execute(
            """
            SELECT
              a.attname AS column_name
            FROM
              pg_index i
            JOIN
              pg_attribute a
              ON a.attrelid = i.indrelid
              AND a.attnum = ANY(i.indkey)
            WHERE
              i.indrelid = 'to_repack'::regclass
              AND i.indisprimary
            ORDER BY
              array_position(i.indkey, a.attnum);
        """
        )
        pk_columns = [row[0] for row in cur.fetchall()]
        assert pk_columns == ["id", "datetime_field"]

        # Verify data integrity
        cur.execute("SELECT COUNT(*) FROM to_repack;")
        count = cur.fetchone()
        assert count is not None
        assert count[0] == 101, "All rows should be present (100 + 1 inserted)"


def test_partition_calling_stages_individually(
    connection: _psycopg.Connection,
) -> None:
    """
    Test that partitioning works when calling stages individually (e.g., swap()
    directly) rather than using full(). This simulates resuming a repack process.
    """
    with _cur.get_cursor(connection) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
        )
        # Clean up any referring tables from other tests
        cur.execute("DROP TABLE IF EXISTS referring_table CASCADE;")
        cur.execute("DROP TABLE IF EXISTS not_valid_referring_table CASCADE;")
        cur.execute("DROP TABLE IF EXISTS other_referring_table CASCADE;")

        # Create the Psycopack instance with partition config
        repack = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
            partition_config=_partition.PartitionConfig(
                column="datetime_field",
                num_of_extra_partitions_ahead=3,
                strategy=_partition.DateRangeStrategy(
                    partition_by=_partition.PartitionInterval.MONTH
                ),
            ),
        )

        # Call stages individually
        repack.pre_validate()
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        repack.post_sync_update()

        # Now instantiate a new Psycopack and call swap() directly
        # (simulating resuming from a previous run)
        repack2 = Psycopack(
            table="to_repack",
            batch_size=1,
            conn=connection,
            cur=cur,
            partition_config=_partition.PartitionConfig(
                column="datetime_field",
                num_of_extra_partitions_ahead=3,
                strategy=_partition.DateRangeStrategy(
                    partition_by=_partition.PartitionInterval.MONTH
                ),
            ),
        )
        repack2.swap()
        repack2.clean_up()

        # Verify the table is now partitioned
        cur.execute("""
            SELECT
              pt.relname AS partition_name
            FROM
              pg_inherits
            JOIN
              pg_class pt
              ON pt.oid = inhrelid
            JOIN
              pg_class p
              ON p.oid = inhparent
            WHERE
              p.relname = 'to_repack'
            ORDER BY
              partition_name;
        """)
        partitions = cur.fetchall()
        assert len(partitions) > 0, "Table should have partitions"


def test_pk_column_property_accessed_before_setup(
    connection: _psycopg.Connection,
) -> None:
    """
    Test pk_column property when accessed before setup_repacking.
    """
    with _cur.get_cursor(connection, logged=True) as cur:
        # Create a simple table
        cur.execute(
            """
            CREATE TABLE test_pk_early_access (
                id bigint PRIMARY KEY,
                data text
            );
            """
        )
        cur.execute("INSERT INTO test_pk_early_access (id, data) VALUES (1, 'test');")
        connection.commit()

        try:
            # Create Psycopack instance
            repack = Psycopack(
                table="test_pk_early_access",
                batch_size=1,
                conn=connection,
                cur=cur,
            )
            # Access pk_column BEFORE calling setup_repacking
            pk_col = repack.pk_column
            assert pk_col == "id"
            # Verify _pk_column was cached
            assert repack.pk_column == "id"
        finally:
            connection.rollback()
            cur.execute("DROP TABLE IF EXISTS test_pk_early_access CASCADE;")
            connection.commit()


def test_pk_column_property_for_partitioned_table(
    connection: _psycopg.Connection,
) -> None:
    """
    Test pk_column property when _pk_column is None for partitioned tables.

    This tests the code path where pk_column is accessed before setup_repacking
    and handles composite primary keys (original PK + partition column).
    """
    with _cur.get_cursor(connection, logged=True) as cur:
        # Create a simple table with date column for partitioning
        cur.execute(
            """
            CREATE TABLE test_pk_column (
                id bigint PRIMARY KEY,
                datetime_field date NOT NULL,
                data text
            );
            """
        )
        cur.execute(
            "INSERT INTO test_pk_column (id, datetime_field, data) "
            "VALUES (1, '2025-01-01', 'test');"
        )
        connection.commit()
        try:
            # Create Psycopack instance with partitioning
            repack = Psycopack(
                table="test_pk_column",
                batch_size=1,
                conn=connection,
                cur=cur,
                partition_config=_partition.PartitionConfig(
                    column="datetime_field",
                    num_of_extra_partitions_ahead=1,
                    strategy=_partition.DateRangeStrategy(
                        partition_by=_partition.PartitionInterval.DAY
                    ),
                ),
                sync_strategy=SyncStrategy.CHANGE_LOG,
            )

            # Validate and setup to create the partitioned table
            repack.pre_validate()
            repack.setup_repacking()

            # Now test pk_column property - for partitioned tables,
            # the PK becomes composite (original PK + partition column)
            # but pk_column should return the first column (original PK)
            pk_col = repack.pk_column
            assert pk_col == "id"

            # Verify the copy table was created as partitioned
            cur.execute(
                f"""
                SELECT relkind FROM pg_class
                WHERE relname = '{repack.copy_table}'
                AND relnamespace = 'public'::regnamespace;
                """
            )
            result = cur.fetchone()
            assert result is not None
            assert result[0] == "p"  # 'p' means partitioned table

        finally:
            connection.rollback()
            cur.execute("DROP TABLE IF EXISTS test_pk_column CASCADE;")
            connection.commit()


def test_full_method_resume_from_swap_stage(
    connection: _psycopg.Connection,
) -> None:
    """
    Test the full() method when resuming from SWAP stage.
    """
    with _cur.get_cursor(connection, logged=True) as cur:
        # Create a test table
        table_name = "test_resume_swap"
        cur.execute(f"CREATE TABLE {table_name} (id bigint PRIMARY KEY, data text);")
        cur.execute(f"INSERT INTO {table_name} (id, data) VALUES (1, 'test');")
        connection.commit()

        try:
            repack = Psycopack(
                table=table_name,
                batch_size=100,
                conn=connection,
                cur=cur,
            )

            # Run through to POST_SYNC_UPDATE stage (but not SWAP)
            repack.pre_validate()
            repack.setup_repacking()
            repack.backfill()
            repack.sync_schemas()
            repack.post_sync_update()

            # Verify we're at SWAP stage (next to be completed)
            current_stage = repack.tracker.get_current_stage()
            assert current_stage == _tracker.Stage.SWAP

            # Now call full() - it should run swap and clean_up
            repack.full()

            # Verify we've completed everything - after clean_up, the tracker row
            # is deleted, so we're back at PRE_VALIDATION stage
            current_stage = repack.tracker.get_current_stage()
            assert current_stage == _tracker.Stage.PRE_VALIDATION

        finally:
            connection.rollback()
            # Clean up any remaining tables
            cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            cur.execute(
                f"DROP TABLE IF EXISTS {table_name}_psycopack_repacked CASCADE;"
            )
            connection.commit()
