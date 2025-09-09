from textwrap import dedent

from psycopack import _cur, _psycopg


def create_table_for_repacking(
    connection: _psycopg.Connection,
    cur: _cur.Cursor,
    table_name: str = "to_repack",
    rows: int = 100,
    referred_table_rows: int = 10,
    referring_table_rows: int = 10,
    with_exclusion_constraint: bool = False,
    pk_type: str = "SERIAL",
    pk_name: str = "id",
    pk_start: int = 1,
    ommit_sequence: bool = False,
    schema: str = "public",
) -> None:
    """
    Creates a table to repack (default: "to_repack") that has:
        1. A field with a btree index.
        2. A field with a varchar_pattern_ops index.
        3. A field with a unique index.
        4. A field with a unique constraint.
        5. Foreign keys to other tables (valid and not valid).
        6. Tables that have foreign keys to it (valid and not valid).
        7. Check constraints (valid and not valid).
        8. A column that has the same name as the table.
    """
    cur.execute(f"CREATE TABLE {schema}.referred_table (id SERIAL PRIMARY KEY);")
    cur.execute(
        f"INSERT INTO {schema}.referred_table (id) SELECT generate_series(1, {referred_table_rows});"
    )
    cur.execute(
        f"CREATE TABLE {schema}.not_valid_referred_table (id SERIAL PRIMARY KEY);"
    )
    cur.execute(
        f"INSERT INTO {schema}.not_valid_referred_table (id) SELECT generate_series(1, {referred_table_rows});"
    )

    if (
        "serial" not in pk_type.lower()
        and "identity" not in pk_type.lower()
        and not ommit_sequence
    ):
        # Create a sequence manually.
        seq = f"{table_name}_seq"
        cur.execute(
            f"CREATE SEQUENCE {schema}.{seq} MINVALUE {pk_start} START WITH {pk_start};"
        )
        pk_type = f"{pk_type} DEFAULT NEXTVAL('{schema}.{seq}')"

    cur.execute(
        dedent(f"""
        CREATE TABLE {schema}.{table_name} (
            {pk_name} {pk_type} PRIMARY KEY,
            var_with_btree VARCHAR(255),
            var_with_pattern_ops VARCHAR(255),
            int_with_check INTEGER CHECK (int_with_check >= 0),
            int_with_not_valid_check INTEGER,
            int_with_long_index_name INTEGER,
            var_with_unique_idx VARCHAR(10),
            var_with_unique_const VARCHAR(10) UNIQUE,
            valid_fk INTEGER REFERENCES {schema}.referred_table(id),
            not_valid_fk INTEGER,
            {table_name} INTEGER,
            var_maybe_with_exclusion VARCHAR(255),
            var_with_multiple_idx VARCHAR(10)
        );
    """)
    )
    if "serial" in pk_type.lower():
        seq = f"{table_name}_{pk_name}_seq"
        cur.execute(
            f"ALTER SEQUENCE {schema}.{seq} MINVALUE {pk_start} RESTART WITH {pk_start};"
        )

    cur.execute(f"CREATE INDEX btree_idx ON {schema}.{table_name} (var_with_btree);")
    cur.execute(
        f"CREATE INDEX pattern_ops_idx ON {schema}.{table_name} (var_with_pattern_ops varchar_pattern_ops);"
    )
    cur.execute(
        f"CREATE UNIQUE INDEX unique_idx ON {schema}.{table_name} (var_with_unique_idx);"
    )
    cur.execute(
        dedent(f"""
        CREATE INDEX
        this_carefully_crafted_index_name_has_exactly_sixty_three_chars
        ON {schema}.{table_name} (int_with_long_index_name);
        """)
    )

    cur.execute(
        f"CREATE INDEX duplicate_idx_1 ON {schema}.{table_name} (var_with_multiple_idx);"
    )
    cur.execute(
        f"CREATE INDEX duplicate_idx_2 ON {schema}.{table_name} (var_with_multiple_idx);"
    )
    cur.execute(
        f"CREATE UNIQUE INDEX duplicate_idx_3 ON {schema}.{table_name} (var_with_multiple_idx);"
    )

    # Index for a column that has the same name as the table.
    cur.execute(
        f"CREATE INDEX {table_name}_idx ON {schema}.{table_name} ({table_name});"
    )
    cur.execute(
        dedent(f"""
        ALTER TABLE {schema}.{table_name} ADD CONSTRAINT not_valid_fk
            FOREIGN KEY (not_valid_fk)
            REFERENCES {schema}.not_valid_referred_table(id)
            NOT VALID;
    """)
    )
    cur.execute(
        dedent(f"""
        ALTER TABLE {schema}.{table_name} ADD CONSTRAINT not_valid_check
        CHECK (int_with_not_valid_check >= 0) NOT VALID;
        """)
    )
    # Constraint for a column that has the same name as the table.
    cur.execute(
        dedent(f"""
        ALTER TABLE {schema}.{table_name} ADD CONSTRAINT {table_name}_const
        CHECK ({table_name} > 0);
        """)
    )
    if with_exclusion_constraint:
        cur.execute(
            dedent(f"""
            ALTER TABLE {schema}.{table_name}
            ADD CONSTRAINT exclusion_var
            EXCLUDE USING BTREE (var_maybe_with_exclusion WITH =);
            """)
        )

    cur.execute(
        dedent(f"""
        INSERT INTO {schema}.{table_name} (
            {"id," if ommit_sequence else ""}
            var_with_btree,
            var_with_pattern_ops,
            int_with_check,
            int_with_not_valid_check,
            int_with_long_index_name,
            var_with_unique_idx,
            var_with_unique_const,
            valid_fk,
            not_valid_fk,
            {table_name},
            var_maybe_with_exclusion,
            var_with_multiple_idx
        )
        SELECT
            {"gs," if ommit_sequence else ""}
            substring(md5(random()::text), 1, 10),
            substring(md5(random()::text), 1, 10),
            (floor(random() * 10) + 1)::int,
            (floor(random() * 10) + 1)::int,
            (floor(random() * 10) + 1)::int,
            substring(md5(random()::text), 1, 10),
            substring(md5(random()::text), 1, 10),
            (floor(random() * {referred_table_rows}) + 1)::int,
            (floor(random() * {referred_table_rows}) + 1)::int,
            (floor(random() * 10) + 1)::int,
            substring(md5(random()::text), 1, 10),
            substring(md5(random()::text), 1, 10)
        FROM generate_series(1, {rows}) AS gs;
    """)
    )
    cur.execute(
        dedent(f"""
        CREATE TABLE {schema}.referring_table (
            id SERIAL PRIMARY KEY,
            {table_name}_{pk_name} INTEGER REFERENCES {schema}.{table_name}({pk_name})
        );
    """)
    )
    cur.execute(
        dedent(f"""
        INSERT INTO {schema}.referring_table ({table_name}_{pk_name})
        SELECT generate_series({pk_start}, {pk_start + referring_table_rows - 1});
    """)
    )
    cur.execute(
        dedent(f"""
        CREATE TABLE {schema}.not_valid_referring_table (
            id SERIAL PRIMARY KEY,
            {table_name}_{pk_name} INTEGER
        );
    """)
    )
    cur.execute(
        dedent(f"""
        ALTER TABLE {schema}.not_valid_referring_table ADD CONSTRAINT not_valid_referring
            FOREIGN KEY ({table_name}_{pk_name})
            REFERENCES {schema}.{table_name}({pk_name})
            NOT VALID;
    """)
    )
    cur.execute(
        dedent(f"""
        INSERT INTO {schema}.not_valid_referring_table ({table_name}_{pk_name})
        SELECT generate_series({pk_start}, {pk_start + referring_table_rows - 1});
    """)
    )


def create_table_for_backfilling(
    cur: _cur.Cursor,
    table_name: str = "to_backfill",
    positive_rows: int = 100,
    negative_rows: int = 100,
) -> None:
    cur.execute(
        dedent(f"""
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY
            );
        """)
    )

    if positive_rows > 0:
        cur.execute(
            dedent(f"""
                INSERT INTO {table_name} (id) SELECT generate_series(1, {positive_rows});
            """)
        )

    if negative_rows > 0:
        negative_base = -1000  # arbitrary
        cur.execute(
            dedent(f"""
                INSERT INTO {table_name} (id)
                SELECT generate_series({negative_base}, {negative_base} + {negative_rows} - 1);
            """)
        )
