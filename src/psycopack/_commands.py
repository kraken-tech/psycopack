import datetime
import hashlib
from contextlib import contextmanager
from textwrap import dedent
from typing import Iterator

from . import _cur, _introspect, _partition
from . import _psycopg as psycopg


class Command:
    def __init__(
        self,
        *,
        conn: psycopg.Connection,
        cur: _cur.Cursor,
        introspector: _introspect.Introspector,
        schema: str,
        partition_config: _partition.PartitionConfig | None = None,
    ) -> None:
        self.conn = conn
        self.cur = cur
        self.introspector = introspector
        self.schema = schema
        self.partition_config = partition_config

    def drop_constraint(self, *, table: str, constraint: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                ALTER TABLE {schema}.{table}
                DROP CONSTRAINT {constraint};
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                constraint=psycopg.sql.Identifier(constraint),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def drop_table_if_exists(self, *, table: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("DROP TABLE IF EXISTS {schema}.{table}")
            .format(
                table=psycopg.sql.Identifier(table),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def create_copy_table(self, *, base_table: str, copy_table: str) -> None:
        if self.partition_config:
            return self._create_partitioned_table(
                base_table=base_table, copy_table=copy_table
            )

        # Create a non-partitioned table (default behavior)
        self._create_non_partitioned_table(base_table=base_table, copy_table=copy_table)

    def _create_non_partitioned_table(
        self, *, base_table: str, copy_table: str
    ) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE TABLE {schema}.{copy_table}
                (LIKE {schema}.{table} INCLUDING DEFAULTS);
                """)
            )
            .format(
                table=psycopg.sql.Identifier(base_table),
                copy_table=psycopg.sql.Identifier(copy_table),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def _create_partitioned_table(self, *, base_table: str, copy_table: str) -> None:
        assert self.partition_config is not None
        assert isinstance(self.partition_config.strategy, _partition.DateRangeStrategy)

        # Create the parent partitioned table
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE TABLE {schema}.{copy_table}
                (LIKE {schema}.{table} INCLUDING DEFAULTS)
                PARTITION BY RANGE ({partition_column});
                """)
            )
            .format(
                table=psycopg.sql.Identifier(base_table),
                copy_table=psycopg.sql.Identifier(copy_table),
                schema=psycopg.sql.Identifier(self.schema),
                partition_column=psycopg.sql.Identifier(self.partition_config.column),
            )
            .as_string(self.conn)
        )

        # Create partitions ahead of time
        self._create_partitions(base_table=base_table, copy_table=copy_table)

    def _create_partitions(self, *, base_table: str, copy_table: str) -> None:
        assert self.partition_config is not None
        strategy = self.partition_config.strategy
        assert isinstance(strategy, _partition.DateRangeStrategy)

        num_of_extra_partitions = self.partition_config.num_of_extra_partitions_ahead

        min_value = self.introspector.get_min_partition_date_value(
            table=base_table, column=self.partition_config.column
        )
        max_value = self.introspector.get_max_partition_date_value(
            table=base_table, column=self.partition_config.column
        )
        partition_start = self._get_first_partition_start_date(
            min_value=min_value, strategy=strategy
        )
        partition_end = self._get_last_partition_end_date(
            max_value=max_value,
            strategy=strategy,
            num_of_extra_partitions=num_of_extra_partitions,
        )

        # Create partitions from partition_start to partition_end
        current_partition_start = partition_start

        while current_partition_start < partition_end:
            partition_suffix = self._get_partition_suffix(
                current_partition_start=current_partition_start, strategy=strategy
            )

            current_partition_end = self._get_partition_end_boundary(
                current_partition_start=current_partition_start, strategy=strategy
            )
            self._create_datetime_partition(
                base_table=base_table,
                copy_table=copy_table,
                partition_suffix=partition_suffix,
                start=current_partition_start,
                end=current_partition_end,
            )
            current_partition_start = current_partition_end

    def _get_first_partition_start_date(
        self, *, min_value: datetime.date, strategy: _partition.DateRangeStrategy
    ) -> datetime.date:
        """
        Align the minimum value to partition boundaries.
        For DAY: uses the exact min_value
        For MONTH: aligns to the first day of the month
        """
        if strategy.partition_by == _partition.PartitionInterval.DAY:
            return min_value
        elif strategy.partition_by == _partition.PartitionInterval.MONTH:
            # Align to start of month
            return min_value.replace(day=1)
        else:  # pragma: no cover
            raise NotImplementedError

    def _get_last_partition_end_date(
        self,
        *,
        max_value: datetime.date,
        strategy: _partition.DateRangeStrategy,
        num_of_extra_partitions: int,
    ) -> datetime.date:
        """
        Calculate the end date for partitioning.
        Always adds 1 interval unit to cover max_value (since range partitions are
        exclusive on the upper bound), plus num_of_extra_partitions.
        For DAY: adds 1 + num_of_extra_partitions days
        For MONTH: adds 1 + num_of_extra_partitions months
        """
        if strategy.partition_by == _partition.PartitionInterval.DAY:
            # Add 1 day to ensure max_value is covered, plus extra partitions
            return max_value + datetime.timedelta(days=1 + num_of_extra_partitions)
        elif strategy.partition_by == _partition.PartitionInterval.MONTH:
            # Add 1 month to ensure max_value is covered, plus extra partitions
            # Add months by advancing to first of month and adding 32*months,
            # then normalising. This is because timedelta doesn't accept
            # "months" as argument.
            temp_date = max_value.replace(day=1)
            for _ in range(1 + num_of_extra_partitions):
                temp_date = (temp_date + datetime.timedelta(days=32)).replace(day=1)
            return temp_date
        else:  # pragma: no cover
            raise NotImplementedError

    def _get_partition_end_boundary(
        self,
        *,
        current_partition_start: datetime.date,
        strategy: _partition.DateRangeStrategy,
    ) -> datetime.date:
        """
        Calculate the end boundary for a single partition.
        For DAY: adds 1 day
        For MONTH: advances to the first day of the next month
        """
        if strategy.partition_by == _partition.PartitionInterval.DAY:
            return current_partition_start + datetime.timedelta(days=1)
        elif strategy.partition_by == _partition.PartitionInterval.MONTH:
            # Next month boundary
            return (current_partition_start + datetime.timedelta(days=32)).replace(
                day=1
            )
        else:  # pragma: no cover
            raise NotImplementedError

    def _get_partition_suffix(
        self,
        *,
        current_partition_start: datetime.date,
        strategy: _partition.DateRangeStrategy,
    ) -> str:
        """
        Generate a date-based partition suffix.
        For DAY: returns p20250101 (YYYYMMDD format)
        For MONTH: returns p202501 (YYYYMM format)
        """
        if strategy.partition_by == _partition.PartitionInterval.DAY:
            # Format: p20250101 (YYYYMMDD)
            return f"p{current_partition_start.strftime('%Y%m%d')}"
        elif strategy.partition_by == _partition.PartitionInterval.MONTH:
            # Format: p202501 (YYYYMM)
            return f"p{current_partition_start.strftime('%Y%m')}"
        else:  # pragma: no cover
            raise NotImplementedError

    def _create_datetime_partition(
        self,
        *,
        base_table: str,
        copy_table: str,
        partition_suffix: str,
        start: datetime.date,
        end: datetime.date,
    ) -> None:
        """Create a single datetime range partition."""
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE TABLE {schema}.{partition_name}
                PARTITION OF {schema}.{copy_table}
                FOR VALUES FROM ({start}) TO ({end});
                """)
            )
            .format(
                schema=psycopg.sql.Identifier(self.schema),
                partition_name=psycopg.sql.Identifier(
                    f"{base_table}_{partition_suffix}"
                ),
                copy_table=psycopg.sql.Identifier(copy_table),
                start=psycopg.sql.Literal(start),
                end=psycopg.sql.Literal(end),
            )
            .as_string(self.conn)
        )

    def drop_sequence_if_exists(self, *, seq: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("DROP SEQUENCE IF EXISTS {schema}.{seq};")
            .format(
                seq=psycopg.sql.Identifier(seq),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def create_sequence(self, *, seq: str, bigint: bool, minvalue: int) -> None:
        if bigint:
            sql = "CREATE SEQUENCE {schema}.{seq} AS BIGINT MINVALUE {minvalue};"
        else:
            sql = "CREATE SEQUENCE {schema}.{seq} MINVALUE {minvalue};"

        self.cur.execute(
            psycopg.sql.SQL(sql)
            .format(
                seq=psycopg.sql.Identifier(seq),
                schema=psycopg.sql.Identifier(self.schema),
                minvalue=psycopg.sql.Literal(minvalue),
            )
            .as_string(self.conn)
        )

    def set_table_id_seq(self, *, table: str, seq: str, pk_column: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                ALTER TABLE {schema}.{table}
                ALTER COLUMN {pk_column}
                SET DEFAULT nextval('{schema}.{seq}');
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                pk_column=psycopg.sql.Identifier(pk_column),
                seq=psycopg.sql.Identifier(seq),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def add_pk(self, *, table: str, pk_column: str) -> None:
        # For partitioned tables, the PK must include all partitioning columns
        if self.partition_config:
            pk_columns = psycopg.sql.SQL(", ").join(
                [
                    psycopg.sql.Identifier(pk_column),
                    psycopg.sql.Identifier(self.partition_config.column),
                ]
            )
            self.cur.execute(
                psycopg.sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({pk_columns});"
                )
                .format(
                    table=psycopg.sql.Identifier(table),
                    pk_columns=pk_columns,
                    schema=psycopg.sql.Identifier(self.schema),
                )
                .as_string(self.conn)
            )
        else:
            self.cur.execute(
                psycopg.sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({pk_column});"
                )
                .format(
                    table=psycopg.sql.Identifier(table),
                    pk_column=psycopg.sql.Identifier(pk_column),
                    schema=psycopg.sql.Identifier(self.schema),
                )
                .as_string(self.conn)
            )

    def create_copy_function(
        self,
        *,
        function: str,
        table_from: str,
        table_to: str,
        columns: list[str],
        pk_column: str,
    ) -> None:
        # Note: assumes "id" to be the primary key.
        # TODO: generalise so other PK types can work.
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE OR REPLACE FUNCTION {schema}.{function}(BIGINT, BIGINT)
                RETURNS VOID AS $$

                  INSERT INTO {schema}.{table_to}
                  OVERRIDING SYSTEM VALUE
                  SELECT {columns}
                  FROM {schema}.{table_from}
                  WHERE {pk_column} BETWEEN $1 AND $2
                  ON CONFLICT DO NOTHING

                $$ LANGUAGE SQL SECURITY DEFINER;
                """)
            )
            .format(
                function=psycopg.sql.Identifier(function),
                table_from=psycopg.sql.Identifier(table_from),
                table_to=psycopg.sql.Identifier(table_to),
                columns=psycopg.sql.SQL(",").join(
                    [psycopg.sql.Identifier(c) for c in columns]
                ),
                pk_column=psycopg.sql.Identifier(pk_column),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def drop_trigger_if_exists(self, *, table: str, trigger: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("DROP TRIGGER IF EXISTS {trigger} ON {schema}.{table}")
            .format(
                trigger=psycopg.sql.Identifier(trigger),
                table=psycopg.sql.Identifier(table),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def create_source_to_copy_trigger(
        self,
        trigger_name: str,
        function: str,
        table_from: str,
        table_to: str,
        pk_column: str,
    ) -> None:
        # Note: takes a ShareRowExclusiveLock on the table_from.
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE OR REPLACE FUNCTION {schema}.{trigger_name}()
                RETURNS TRIGGER AS
                $$
                BEGIN
                  IF ( TG_OP = 'INSERT') THEN
                    PERFORM {schema}.{function}(NEW.{pk_column}, NEW.{pk_column});
                    RETURN NEW;
                  ELSIF ( TG_OP = 'UPDATE') THEN
                    DELETE FROM {schema}.{table_to} WHERE {pk_column} = OLD.{pk_column};
                    PERFORM {schema}.{function}(NEW.{pk_column}, NEW.{pk_column});
                    RETURN NEW;
                  ELSIF ( TG_OP = 'DELETE') THEN
                    DELETE FROM {schema}.{table_to} WHERE {pk_column} = OLD.{pk_column};
                    RETURN OLD;
                  END IF;
                END;
                $$ LANGUAGE PLPGSQL SECURITY DEFINER;

                CREATE TRIGGER {trigger_name}
                AFTER INSERT OR UPDATE OR DELETE ON {schema}.{table_from}
                FOR EACH ROW EXECUTE PROCEDURE {schema}.{trigger_name}();
                """)
            )
            .format(
                trigger_name=psycopg.sql.Identifier(trigger_name),
                function=psycopg.sql.Identifier(function),
                table_from=psycopg.sql.Identifier(table_from),
                table_to=psycopg.sql.Identifier(table_to),
                pk_column=psycopg.sql.Identifier(pk_column),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def create_backfill_log(self, *, table: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE TABLE {schema}.{table} (
                  id SERIAL PRIMARY KEY,
                  batch_start BIGINT,
                  batch_end BIGINT,
                  finished BOOLEAN
                );
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE INDEX {log_index}
                ON {schema}.{table} ("finished")
                WHERE finished IS FALSE;
                """)
            )
            .format(
                log_index=psycopg.sql.Identifier(f"{table}_idx"),
                table=psycopg.sql.Identifier(table),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def create_change_log(self, *, src_table: str, change_log: str) -> None:
        pk_info = self.introspector.get_primary_key_info(table=src_table)

        # At this point tables without primary-keys or with composite keys have
        # already been stopped of proceeding as they aren't supported by
        # Psycopack.
        assert pk_info is not None
        assert len(pk_info.columns) == 1
        assert len(pk_info.data_types) == 1

        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE TABLE {schema}.{table} (
                  id BIGSERIAL PRIMARY KEY,
                  src_pk {data_type} UNIQUE NOT NULL
                );
                """)
            )
            .format(
                table=psycopg.sql.Identifier(change_log),
                schema=psycopg.sql.Identifier(self.schema),
                data_type=psycopg.sql.SQL(pk_info.data_types[0]),
            )
            .as_string(self.conn)
        )

    def create_change_log_function(
        self,
        *,
        function: str,
        table_from: str,
        table_to: str,
    ) -> None:
        # Note: assumes the PK to be an integer-like type that can be
        # automatically promoted to bigint.
        # TODO: generalise for other PK types.
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE OR REPLACE FUNCTION {schema}.{function}(BIGINT)
                RETURNS VOID AS $$

                  INSERT INTO {schema}.{table_to}
                  (src_pk) VALUES ($1)
                  ON CONFLICT DO NOTHING

                $$ LANGUAGE SQL SECURITY DEFINER;
                """)
            )
            .format(
                function=psycopg.sql.Identifier(function),
                table_from=psycopg.sql.Identifier(table_from),
                table_to=psycopg.sql.Identifier(table_to),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def create_change_log_copy_function(
        self,
        *,
        function: str,
        table_from: str,
        table_to: str,
        change_log: str,
        columns: list[str],
        pk_column: str,
    ) -> None:
        # Note: assumes the PK to be an integer-like type that can be
        # automatically promoted to bigint.
        # TODO: generalise for other PK types.
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE OR REPLACE FUNCTION {schema}.{function}(batch_size BIGINT)
                RETURNS VOID
                LANGUAGE plpgsql
                SECURITY DEFINER
                AS $$
                DECLARE
                  pks BIGINT[];
                BEGIN
                  -- Lock change log rows FIRST, then aggregate
                  SELECT
                    ARRAY_AGG(src_pk)
                  INTO
                    pks
                  FROM (
                    SELECT
                      src_pk
                    FROM
                      {schema}.{change_log}
                    ORDER BY
                      src_pk
                    LIMIT
                      batch_size
                    FOR UPDATE SKIP LOCKED
                  ) locked_rows;

                  -- Nothing to do
                  IF pks IS NULL THEN
                    RETURN;
                  END IF;

                  -- Lock source rows
                  PERFORM 1
                  FROM {schema}.{table_from}
                  WHERE {pk_column} = ANY (pks)
                  FOR UPDATE;

                  -- Lock destination rows
                  PERFORM 1
                  FROM {schema}.{table_to}
                  WHERE {pk_column} = ANY (pks)
                  FOR UPDATE;

                  -- Delete destination rows
                  DELETE FROM {schema}.{table_to}
                  WHERE {pk_column} = ANY (pks);

                  -- Delete change log rows
                  DELETE FROM {schema}.{change_log}
                  WHERE src_pk = ANY (pks);

                  -- Insert from source
                  INSERT INTO {schema}.{table_to}
                  OVERRIDING SYSTEM VALUE
                  SELECT {columns}
                  FROM {schema}.{table_from}
                  WHERE {pk_column} = ANY (pks);
                END;
                $$;
                """)
            )
            .format(
                function=psycopg.sql.Identifier(function),
                table_from=psycopg.sql.Identifier(table_from),
                table_to=psycopg.sql.Identifier(table_to),
                change_log=psycopg.sql.Identifier(change_log),
                schema=psycopg.sql.Identifier(self.schema),
                columns=psycopg.sql.SQL(",").join(
                    [psycopg.sql.Identifier(c) for c in columns]
                ),
                pk_column=psycopg.sql.Identifier(pk_column),
            )
            .as_string(self.conn)
        )

    def create_change_log_trigger(
        self,
        *,
        trigger_name: str,
        table_from: str,
        table_to: str,
        pk_column: str,
        function: str,
    ) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                CREATE OR REPLACE FUNCTION {schema}.{trigger_name}()
                RETURNS TRIGGER AS
                $$
                BEGIN
                  IF ( TG_OP = 'INSERT') THEN
                    PERFORM {schema}.{function}(NEW.{pk_column});
                    RETURN NEW;
                  ELSIF ( TG_OP = 'UPDATE') THEN
                    PERFORM {schema}.{function}(NEW.{pk_column});
                    RETURN NEW;
                  ELSIF ( TG_OP = 'DELETE') THEN
                    PERFORM {schema}.{function}(OLD.{pk_column});
                    RETURN OLD;
                  END IF;
                END;
                $$ LANGUAGE PLPGSQL SECURITY DEFINER;

                CREATE TRIGGER {trigger_name}
                AFTER INSERT OR UPDATE OR DELETE ON {schema}.{table_from}
                FOR EACH ROW EXECUTE PROCEDURE {schema}.{trigger_name}();
                """)
            )
            .format(
                trigger_name=psycopg.sql.Identifier(trigger_name),
                function=psycopg.sql.Identifier(function),
                table_from=psycopg.sql.Identifier(table_from),
                table_to=psycopg.sql.Identifier(table_to),
                pk_column=psycopg.sql.Identifier(pk_column),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    @contextmanager
    def session_lock(self, *, name: str) -> Iterator[None]:
        # Based on:
        # https://github.com/Opus10/django-pglock/blob/bf7422d3a74eed8196e13f6b28b72fb0623560e5/pglock/core.py#L137-L139
        key = int.from_bytes(
            hashlib.sha256(name.encode("utf-8")).digest()[:8], "little", signed=True
        )
        self.cur.execute(f"SELECT pg_advisory_lock({key});")
        yield
        self.cur.execute(f"SELECT pg_advisory_unlock({key});")

    def populate_backfill_log(
        self,
        table: str,
        batch_size: int,
        min_pk: int,
        max_pk: int,
    ) -> None:
        batches = (
            (batch_start, min(batch_start + batch_size - 1, max_pk), False)
            for batch_start in range(min_pk, max_pk + 1, batch_size)
        )
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                INSERT INTO {schema}.{table} (batch_start, batch_end, finished)
                VALUES {batches};
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                batches=psycopg.sql.SQL(", ").join(
                    map(psycopg.sql.SQL, [str(batch) for batch in batches])
                ),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def create_unique_constraint_using_idx(
        self,
        table: str,
        constraint: str,
        index: str,
    ) -> None:
        add_constraint_sql = dedent("""
            ALTER TABLE {schema}.{table}
            ADD CONSTRAINT {constraint}
            UNIQUE USING INDEX {index} NOT DEFERRABLE
        """)

        self.cur.execute(
            psycopg.sql.SQL(add_constraint_sql)
            .format(
                table=psycopg.sql.Identifier(table),
                constraint=psycopg.sql.Identifier(constraint),
                index=psycopg.sql.Identifier(index),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def create_not_valid_constraint_from_def(
        self, *, table: str, constraint: str, definition: str, is_validated: bool
    ) -> None:
        # For partitioned tables, we can't use NOT VALID on foreign keys
        # So we need to remove it from the definition
        is_fk = "FOREIGN KEY" in definition.upper()
        if self.partition_config and is_fk and not is_validated:
            # Remove NOT VALID from the definition for partitioned tables
            definition = definition.replace(" NOT VALID", "").replace("NOT VALID", "")

        add_constraint_sql = dedent("""
            ALTER TABLE {schema}.{table}
            ADD CONSTRAINT {constraint}
            {definition}
        """)
        # Only add NOT VALID if:
        # 1. The constraint is validated (so we make it NOT VALID temporarily)
        # 2. AND it's not a FK on a partitioned table (which doesn't support NOT VALID)
        should_add_not_valid = is_validated and not (self.partition_config and is_fk)

        if should_add_not_valid:
            # If the definition is for a valid constraint, alter it to be not
            # valid manually so that it can be created ONLINE.
            add_constraint_sql += " NOT VALID"
        self.cur.execute(
            psycopg.sql.SQL(add_constraint_sql)
            .format(
                table=psycopg.sql.Identifier(table),
                constraint=psycopg.sql.Identifier(constraint),
                definition=psycopg.sql.SQL(definition),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def validate_constraint(self, *, table: str, constraint: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                ALTER TABLE {schema}.{table}
                VALIDATE CONSTRAINT {constraint}
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                constraint=psycopg.sql.Identifier(constraint),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def drop_function_if_exists(self, *, function: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("DROP FUNCTION IF EXISTS {schema}.{function};")
            .format(
                function=psycopg.sql.Identifier(function),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def rename_table(self, *, table_from: str, table_to: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("ALTER TABLE {schema}.{table_from} RENAME TO {table_to};")
            .format(
                table_from=psycopg.sql.Identifier(table_from),
                table_to=psycopg.sql.Identifier(table_to),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def rename_index(self, *, idx_from: str, idx_to: str) -> None:
        # Note: Takes a ShareUpdateExclusiveLock on idx_from
        self.cur.execute(
            psycopg.sql.SQL("ALTER INDEX {schema}.{idx_from} RENAME TO {idx_to};")
            .format(
                idx_from=psycopg.sql.Identifier(idx_from),
                idx_to=psycopg.sql.Identifier(idx_to),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def rename_sequence(self, *, seq_from: str, seq_to: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("ALTER SEQUENCE {schema}.{seq_from} RENAME TO {seq_to};")
            .format(
                seq_from=psycopg.sql.Identifier(seq_from),
                seq_to=psycopg.sql.Identifier(seq_to),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def rename_constraint(self, *, table: str, cons_from: str, cons_to: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                "ALTER TABLE {schema}.{table} RENAME CONSTRAINT {cons_from} TO {cons_to};"
            )
            .format(
                table=psycopg.sql.Identifier(table),
                cons_from=psycopg.sql.Identifier(cons_from),
                cons_to=psycopg.sql.Identifier(cons_to),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def create_constraint(self, *, table: str, name: str, definition: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                ALTER TABLE {schema}.{table}
                ADD CONSTRAINT {constraint}
                {definition}
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                constraint=psycopg.sql.Identifier(name),
                definition=psycopg.sql.SQL(definition),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def set_generated_identity(
        self, *, table: str, always: bool, pk_column: str
    ) -> None:
        identity_type = "ALWAYS" if always else "BY DEFAULT"
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                ALTER TABLE {schema}.{table}
                ALTER COLUMN {pk_column}
                ADD GENERATED {identity_type} AS IDENTITY;
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                pk_column=psycopg.sql.Identifier(pk_column),
                identity_type=psycopg.sql.SQL(identity_type),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def convert_pk_to_bigint(self, *, table: str, seq: str, pk_column: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                ALTER TABLE {schema}.{table}
                ALTER COLUMN {pk_column}
                SET DATA TYPE BIGINT;
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                pk_column=psycopg.sql.Identifier(pk_column),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def swap_pk_sequence_name(self, *, first_table: str, second_table: str) -> None:
        first_seq = self.introspector.get_pk_sequence_name(table=first_table)
        second_seq = self.introspector.get_pk_sequence_name(table=second_table)
        temp_seq = f"{first_seq}_temp"

        self.rename_sequence(seq_from=first_seq, seq_to=temp_seq)
        self.rename_sequence(seq_from=second_seq, seq_to=first_seq)
        self.rename_sequence(seq_from=temp_seq, seq_to=second_seq)

    def transfer_pk_sequence_value(self, *, source_table: str, dest_table: str) -> None:
        source_seq = self.introspector.get_pk_sequence_name(table=source_table)
        dest_seq = self.introspector.get_pk_sequence_name(table=dest_table)
        value = self.introspector.get_pk_sequence_value(seq=source_seq)

        self.cur.execute(
            psycopg.sql.SQL("SELECT setval('{schema}.{sequence}', {value});")
            .format(
                schema=psycopg.sql.Identifier(self.schema),
                sequence=psycopg.sql.Identifier(dest_seq),
                value=psycopg.sql.SQL(str(value)),
            )
            .as_string(self.conn)
        )

    def update_pk_sequence_value(self, *, table: str) -> None:
        """
        Update the sequence value if it was negative (for use in bigint conversions).
        """
        seq = self.introspector.get_pk_sequence_name(table=table)
        value = self.introspector.get_pk_sequence_value(seq=seq)

        if value < 0:
            # special case handling where negative PK values were used before bigint conversion
            value = 2**31  # reset to positive, specifically the first bigint value

            self.cur.execute(
                psycopg.sql.SQL("SELECT setval('{schema}.{sequence}', {value});")
                .format(
                    schema=psycopg.sql.Identifier(self.schema),
                    sequence=psycopg.sql.Identifier(seq),
                    value=psycopg.sql.SQL(str(value)),
                )
                .as_string(self.conn)
            )

    def acquire_access_exclusive_lock(self, *, table: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("LOCK TABLE {schema}.{table} IN ACCESS EXCLUSIVE MODE;")
            .format(
                table=psycopg.sql.Identifier(table),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def execute_copy_function(
        self, *, function: str, batch: _introspect.BackfillBatch
    ) -> None:
        self.cur.execute(
            psycopg.sql.SQL("SELECT {schema}.{function}({batch_start}, {batch_end});")
            .format(
                function=psycopg.sql.Identifier(function),
                batch_start=psycopg.sql.Literal(batch.start),
                batch_end=psycopg.sql.Literal(batch.end),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def execute_change_log_copy_function(
        self, *, function: str, batch_size: int
    ) -> None:
        self.cur.execute(
            psycopg.sql.SQL("SELECT {schema}.{function}({batch_size});")
            .format(
                function=psycopg.sql.Identifier(function),
                schema=psycopg.sql.Identifier(self.schema),
                batch_size=psycopg.sql.Literal(batch_size),
            )
            .as_string(self.conn)
        )

    def set_batch_to_finished(
        self, *, table: str, batch: _introspect.BackfillBatch
    ) -> None:
        self.cur.execute(
            psycopg.sql.SQL(
                dedent("""
                UPDATE
                  {schema}.{table}
                SET
                  finished = true
                WHERE
                  id = {id};
                """)
            )
            .format(
                table=psycopg.sql.Identifier(table),
                id=psycopg.sql.Literal(batch.id),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def drop_index_concurrently_if_exists(self, *, index: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {schema}.{index};")
            .format(
                index=psycopg.sql.Identifier(index),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    def analyze(self, *, table: str) -> None:
        self.cur.execute(
            psycopg.sql.SQL("ANALYZE {schema}.{table};")
            .format(
                table=psycopg.sql.Identifier(table),
                schema=psycopg.sql.Identifier(self.schema),
            )
            .as_string(self.conn)
        )

    @contextmanager
    def db_transaction(self) -> Iterator[None]:
        self.cur.execute("BEGIN;")
        yield
        self.cur.execute("COMMIT;")

    @contextmanager
    def lock_timeout(self, timeout: datetime.timedelta) -> Iterator[None]:
        timeout_in_ms = int(timeout.total_seconds() * 1000)
        set_sql = f"SET lock_timeout = {timeout_in_ms};"
        reset_sql = "SET lock_timeout TO DEFAULT;"
        self.cur.execute(set_sql)
        yield
        self.cur.execute(reset_sql)
