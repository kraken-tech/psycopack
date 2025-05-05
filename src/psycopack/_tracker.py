import dataclasses
import enum
from contextlib import contextmanager
from textwrap import dedent
from typing import Iterator

from . import _commands, _const, _cur, _introspect
from . import _psycopg as psycopg


class TrackerException(Exception):
    pass


class CannotFindUnfinishedStage(TrackerException):
    pass


class InvalidRowInTrackerTable(TrackerException):
    pass


class TableDoesNotExist(TrackerException):
    pass


class TriggerDoesNotExist(TrackerException):
    pass


class StageAlreadyFinished(TrackerException):
    pass


class InvalidRepackingSetup(TrackerException):
    pass


class InvalidRepackingStep(TrackerException):
    pass


class CannotRevertSwap(TrackerException):
    pass


class FailureDueToLockTimeout(TrackerException):
    pass


@dataclasses.dataclass
class StageInfo:
    name: str
    step: int


class Stage(enum.Enum):
    PRE_VALIDATION = StageInfo(name="PRE_VALIDATION", step=1)
    SETUP = StageInfo(name="SETUP", step=2)
    BACKFILL = StageInfo(name="BACKFILL", step=3)
    SYNC_SCHEMAS = StageInfo(name="SYNC_SCHEMAS", step=4)
    SWAP = StageInfo(name="SWAP", step=5)
    CLEAN_UP = StageInfo(name="CLEAN_UP", step=6)


class Tracker:
    """
    A class to keep track of where in the repacking process a table is.

    It should be used via the two public methods:

      1. `track` (context manager): used to verify if a stage pre-condition
         is valid, and to log the stage completion once it has exited.

      2. `get_current_stage`: used to verify which stage the repacking process
         is currently at.
    """

    def __init__(
        self,
        *,
        table: str,
        conn: psycopg.Connection,
        cur: _cur.LoggedCursor,
        copy_table: str,
        trigger: str,
        backfill_log: str,
        repacked_name: str,
        repacked_trigger: str,
        introspector: _introspect.Introspector,
        command: _commands.Command,
    ) -> None:
        self.table = table
        self.conn = conn
        self.cur = cur
        self.introspector = introspector
        self.command = command

        self.copy_table = copy_table
        self.trigger = trigger
        self.backfill_log = backfill_log
        self.repacked_name = repacked_name
        self.repacked_trigger = repacked_trigger

        self.tracker_table = self._get_tracker_table_name()
        self.tracker_lock = f"{self.tracker_table}_lock"
        self.initial_stage = Stage.PRE_VALIDATION

    @contextmanager
    def track(self, stage: Stage) -> Iterator[None]:
        with self.command.session_lock(name=self.tracker_lock):
            self._ensure_tracker_table_exists(stage=stage)
            self._validate_stage(stage=stage)
            try:
                yield
                self._finish_stage(stage=stage)
            except psycopg.errors.LockNotAvailable as exc:
                raise FailureDueToLockTimeout(
                    f"The stage {stage} failed to complete due to a lock "
                    f"timeout event. Please try to run this same stage again "
                    f"when the table is less busy, or alternatively choose a "
                    f"larger lock timeout value."
                ) from exc

    def get_current_stage(self) -> Stage:
        with self.command.session_lock(name=self.tracker_lock):
            self._ensure_tracker_table_exists()
            return self._get_unfinished_stage()

    def _ensure_tracker_table_exists(self, stage: Stage | None = None) -> None:
        if self._tracker_table_exists():
            return

        if stage and (stage != self.initial_stage):
            raise InvalidRepackingSetup(
                f"The repacking process should be initiated by the "
                f"{self.initial_stage} stage. Tried to start repacking with "
                f"{stage} instead."
            )

        self._create_tracker_table()
        self._set_stage(stage_from=None, stage_to=self.initial_stage)

    def _validate_stage(self, *, stage: Stage) -> None:
        self._validate_tracker_table()
        self._validate_stage_not_finished(stage=stage)
        self._validate_stage_in_right_sequence(stage=stage)
        self._validate_stage_dependencies(stage=stage)

    def _validate_stage_not_finished(self, *, stage: Stage) -> None:
        if self._is_stage_finished(stage=stage):
            raise StageAlreadyFinished(
                f"Stage {stage.value.name} has already completed and cannot run again."
            )

    def _validate_stage_in_right_sequence(self, *, stage: Stage) -> None:
        unfinished_stage = self._get_unfinished_stage()
        if stage != unfinished_stage:
            raise InvalidRepackingStep(
                f"Cannot run {stage} if the current stage is {unfinished_stage}."
            )

    def _validate_stage_dependencies(self, *, stage: Stage) -> None:
        """
        Check if stage dependencies are still valid.

        This validation should uncover inconsistencies, such as a user manually
        deleting a table or a trigger that needs to be used by the passed in
        stage.
        """
        table_dependencies = {
            Stage.SETUP: [self.table],
            Stage.BACKFILL: [self.table, self.copy_table, self.backfill_log],
            Stage.SYNC_SCHEMAS: [self.table, self.copy_table],
            Stage.SWAP: [self.table, self.copy_table],
            Stage.CLEAN_UP: [self.repacked_name, self.table],
        }
        if stage in table_dependencies:
            for table in table_dependencies[stage]:
                self._validate_table_exists(table=table, stage=stage)

        trigger_dependencies = {
            Stage.BACKFILL: [self.trigger],
            Stage.SYNC_SCHEMAS: [self.trigger],
            Stage.SWAP: [self.trigger],
            Stage.CLEAN_UP: [self.repacked_trigger],
        }
        if stage in trigger_dependencies:
            for trigger in trigger_dependencies[stage]:
                self._validate_trigger_exists(trigger=trigger, stage=stage)

    def _finish_stage(self, *, stage: Stage) -> None:
        match stage:
            case Stage.PRE_VALIDATION:
                self._set_stage(stage_from=stage, stage_to=Stage.SETUP)
                return
            case Stage.SETUP:
                self._set_stage(stage_from=stage, stage_to=Stage.BACKFILL)
                return
            case Stage.BACKFILL:
                self._set_stage(stage_from=stage, stage_to=Stage.SYNC_SCHEMAS)
                return
            case Stage.SYNC_SCHEMAS:
                self._set_stage(stage_from=stage, stage_to=Stage.SWAP)
                return
            case Stage.SWAP:
                self._set_stage(stage_from=stage, stage_to=Stage.CLEAN_UP)
                return
            case Stage.CLEAN_UP:
                # Repacking is done. We have to drop the tracker table as well
                # to not leave any repacking relations behind.
                self.command.drop_table_if_exists(table=self.tracker_table)
                return
            case _:  # pragma: no cover
                raise ValueError("Invalid Stage")

    def _set_stage(self, *, stage_from: Stage | None, stage_to: Stage) -> None:
        self._set_latest_stage_to_finished()
        self._insert_tracker_stage(
            stage=stage_to.value.name,
            step=stage_to.value.step,
        )

    def _get_tracker_table_name(self) -> str:
        # Manipulate the copy_table name directly to avoid an extra
        # introspection query to find the table oid.
        oid = self.copy_table.split(f"{_const.NAME_PREFIX}_")[1]
        return f"{_const.NAME_PREFIX}_{oid}_tracker"

    def _tracker_table_exists(self) -> bool:
        return bool(self.introspector.get_table_oid(table=self.tracker_table))

    def _create_tracker_table(self) -> None:
        self.command.drop_table_if_exists(table=self.tracker_table)
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE TABLE {table} (
                  stage VARCHAR(32) NOT NULL UNIQUE,
                  step INTEGER NOT NULL UNIQUE,
                  started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  finished_at TIMESTAMP NULL
                );
                """)
            )
            .format(
                table=psycopg.sql.Identifier(self.tracker_table),
            )
            .as_string(self.conn)
        )
        # The unique index below will guarantee that there is ever only one row
        # in the table with finished_at NULL. This represents the logical
        # constraint of not being possible to carry on two repacking stages at
        # the same time.
        # Due to the limitations of having to support lower versions than
        # Postgres 15, we can't use NULLS NOT DISTINCT and need the workaround
        # below provided by COALESCE.
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE UNIQUE INDEX
                  {index}
                ON
                  {table} (COALESCE(finished_at, '1970-01-01 00:00:00'))
                WHERE
                  finished_at IS NULL;
                """)
            )
            .format(
                index=psycopg.sql.Identifier(f"{self.tracker_table}_uniq_idx"),
                table=psycopg.sql.Identifier(self.tracker_table),
            )
            .as_string(self.conn)
        )

    def _validate_tracker_table(self) -> None:
        """
        Verify that the rows in the table have the expected stage names
        and step numbers in order.

        This ensures that the tracker table hasn't been alterated manually.
        """
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  stage,
                  step
                FROM
                  {table}
                ORDER BY
                  step ASC;
                """)
            )
            .format(table=psycopg.sql.Identifier(self.tracker_table))
            .as_string(self.conn)
        )
        result = self.cur.fetchall()
        expected_stages = sorted(
            [(stage.value.name, stage.value.step) for stage in Stage],
            key=lambda s: s[1],
        )
        if result != expected_stages[: len(result)]:
            raise InvalidRowInTrackerTable(
                f"The tracker table '{self.tracker_table}' has invalid rows."
            )

    def _get_unfinished_stage(self) -> Stage:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  stage
                FROM
                  {table}
                WHERE
                  finished_at IS NULL
                ORDER BY
                  started_at DESC
                LIMIT 1;
                """)
            )
            .format(table=psycopg.sql.Identifier(self.tracker_table))
            .as_string(self.conn)
        )
        result = self.cur.fetchone()
        if not result:
            raise CannotFindUnfinishedStage(
                f"The tracker table '{self.tracker_table}' should have a row "
                "with a null finished_at column. Found none."
            )
        return next((stage for stage in Stage if stage.value.name == result[0]))

    def _is_stage_finished(self, *, stage: Stage) -> bool:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                SELECT
                  1
                FROM
                  {table}
                WHERE
                  stage = {stage}
                  AND finished_at IS NOT NULL;
                """)
            )
            .format(
                table=psycopg.sql.Identifier(self.tracker_table),
                stage=psycopg.sql.Literal(stage.value.name),
            )
            .as_string(self.conn)
        )
        return bool(self.cur.fetchone())

    def _validate_table_exists(self, *, table: str, stage: Stage) -> None:
        if not self.introspector.get_table_oid(table=table):
            raise TableDoesNotExist(
                f"Psycopack {stage.value.name} stage requires the table "
                f"{table} to proceed. However, the table {table} does not exist."
            )

    def _validate_trigger_exists(self, *, trigger: str, stage: Stage) -> None:
        if not self.introspector.trigger_exists(trigger=trigger):
            raise TriggerDoesNotExist(
                f"Psycopack {stage.value.name} stage requires the trigger "
                f"{trigger} to proceed. However, the trigger {trigger} does not exist."
            )

    def _set_latest_stage_to_finished(self) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                UPDATE
                  {table}
                SET
                  finished_at = CURRENT_TIMESTAMP
                WHERE
                  finished_at is NULL;
                """)
            )
            .format(
                table=psycopg.sql.Identifier(self.tracker_table),
            )
            .as_string(self.conn)
        )
        return

    def _insert_tracker_stage(self, *, stage: str, step: int) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                INSERT INTO
                  {table} (stage, step)
                VALUES
                  ({stage}, {step});
                """)
            )
            .format(
                table=psycopg.sql.Identifier(self.tracker_table),
                stage=psycopg.sql.Literal(stage),
                step=psycopg.sql.Literal(step),
            )
            .as_string(self.conn)
        )

    def _revert_swap(self) -> None:
        with self.command.session_lock(name=self.tracker_lock):
            current_stage = self.get_current_stage()
            if current_stage != Stage.CLEAN_UP:
                raise CannotRevertSwap(
                    "Can only revert if a swap was the last completed repacking stage."
                )

            self._delete_current_stage()
            self._reset_stage_finished_at(stage=Stage.SWAP)

    def _delete_current_stage(self) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                DELETE FROM
                  {table}
                WHERE
                  finished_at is NULL;
                """)
            )
            .format(table=psycopg.sql.Identifier(self.tracker_table))
            .as_string(self.conn)
        )

    def _reset_stage_finished_at(self, *, stage: Stage) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                UPDATE
                  {table}
                SET
                  finished_at = NULL
                WHERE
                  stage = {stage};
                """)
            )
            .format(
                table=psycopg.sql.Identifier(self.tracker_table),
                stage=psycopg.sql.Literal(stage.value.name),
            )
            .as_string(self.conn)
        )
