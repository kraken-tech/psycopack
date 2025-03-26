import dataclasses

import pytest

from psycopack import (
    Repack,
    TableDoesNotExist,
    TableIsEmpty,
    _cur,
    _introspect,
    _psycopg,
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
        repack.setup_repacking()
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
        repack.setup_repacking()
        repack.backfill()
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
        repack.setup_repacking()
        repack.backfill()
        repack.sync_schemas()
        repack.full()
        table_after = _collect_table_info(table="to_repack", connection=connection)
        _assert_repack(
            table_before=table_before,
            table_after=table_after,
            repack=repack,
            cur=cur,
        )
