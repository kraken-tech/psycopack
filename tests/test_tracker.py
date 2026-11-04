import pytest

from psycopack import NoPsycopackConversionInProgress, Psycopack, Stage, _cur, _psycopg
from tests import factories


def test_get_current_stage_creates_as_first_stage_if_not_exist(
    connection: _psycopg.Connection,
) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        factories.create_table_for_repacking(
            connection=connection,
            cur=cur,
            table_name="to_repack",
            rows=100,
            pk_type="int",
        )

        cur.execute("CREATE TABLE sample_table (id SERIAL PRIMARY KEY);")

        # easier to make a tracker object with sensible defaults
        # via Psycopack instantiation
        repack = Psycopack(
            table="sample_table",
            batch_size=1,
            conn=connection,
            cur=cur,
            convert_pk_to_bigint=True,
        )

        stage = repack.tracker.get_current_stage(create_if_not_exist=True)

        # process created and set to first stage
        assert stage == Stage.PRE_VALIDATION


def test_get_current_stage_with_create_if_not_exist_false_raises_exception(
    connection: _psycopg.Connection,
) -> None:
    with _cur.get_cursor(connection, logged=True) as cur:
        cur.execute("CREATE TABLE sample_table (id SERIAL PRIMARY KEY);")

        # easier to make a tracker object with sensible defaults
        # via Psycopack instantiation
        repack = Psycopack(
            table="sample_table",
            batch_size=1,
            conn=connection,
            cur=cur,
            convert_pk_to_bigint=True,
        )

        with pytest.raises(NoPsycopackConversionInProgress):
            repack.tracker.get_current_stage(create_if_not_exist=False)
