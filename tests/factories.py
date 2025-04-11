from textwrap import dedent

from psycopack import _psycopg


def create_table_for_repacking(
    connection: _psycopg.Connection,
    cur: _psycopg.Cursor,
    table_name: str = "to_repack",
    rows: int = 100,
    referred_table_rows: int = 10,
    referring_table_rows: int = 10,
    with_exclusion_constraint: bool = False,
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
    cur.execute("CREATE TABLE referred_table (id SERIAL PRIMARY KEY);")
    cur.execute(
        f"INSERT INTO referred_table (id) SELECT generate_series(1, {referred_table_rows});"
    )
    cur.execute("CREATE TABLE not_valid_referred_table (id SERIAL PRIMARY KEY);")
    cur.execute(
        f"INSERT INTO not_valid_referred_table (id) SELECT generate_series(1, {referred_table_rows});"
    )
    cur.execute(
        dedent(f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            var_with_btree VARCHAR(255),
            var_with_pattern_ops VARCHAR(255),
            int_with_check INTEGER CHECK (int_with_check >= 0),
            int_with_not_valid_check INTEGER,
            int_with_long_index_name INTEGER,
            var_with_unique_idx VARCHAR(10),
            var_with_unique_const VARCHAR(10) UNIQUE,
            var_with_deferrable_const VARCHAR(10),
            var_with_deferred_const VARCHAR(10),
            valid_fk INTEGER REFERENCES referred_table(id),
            not_valid_fk INTEGER,
            {table_name} INTEGER,
            var_maybe_with_exclusion VARCHAR(255),
            var_with_multiple_idx VARCHAR(10)
        );
    """)
    )
    cur.execute(f"CREATE INDEX btree_idx ON {table_name} (var_with_btree);")
    cur.execute(
        f"CREATE INDEX pattern_ops_idx ON {table_name} (var_with_pattern_ops varchar_pattern_ops);"
    )
    cur.execute(
        f"CREATE UNIQUE INDEX unique_idx ON {table_name} (var_with_unique_idx);"
    )
    cur.execute(
        dedent(f"""
        CREATE INDEX
        this_carefully_crafted_index_name_has_exactly_sixty_three_chars
        ON {table_name} (int_with_long_index_name);
        """)
    )

    cur.execute(
        f"CREATE INDEX duplicate_idx_1 ON {table_name} (var_with_multiple_idx);"
    )
    cur.execute(
        f"CREATE INDEX duplicate_idx_2 ON {table_name} (var_with_multiple_idx);"
    )
    cur.execute(
        f"CREATE UNIQUE INDEX duplicate_idx_3 ON {table_name} (var_with_multiple_idx);"
    )

    # Index for a column that has the same name as the table.
    cur.execute(f"CREATE INDEX {table_name}_idx ON {table_name} ({table_name});")
    cur.execute(
        dedent(f"""
        ALTER TABLE {table_name} ADD CONSTRAINT not_valid_fk
            FOREIGN KEY (not_valid_fk)
            REFERENCES not_valid_referred_table(id)
            NOT VALID;
    """)
    )
    cur.execute(
        dedent(f"""
        ALTER TABLE {table_name} ADD CONSTRAINT not_valid_check
        CHECK (int_with_not_valid_check >= 0) NOT VALID;
        """)
    )
    cur.execute(
        dedent(f"""
        ALTER TABLE {table_name} ADD CONSTRAINT non_deferrable_const
        UNIQUE (var_with_deferrable_const)
        DEFERRABLE;
        """)
    )
    cur.execute(
        dedent(f"""
        ALTER TABLE {table_name} ADD CONSTRAINT deferred_const
        UNIQUE (var_with_deferred_const)
        DEFERRABLE INITIALLY DEFERRED;
        """)
    )
    # Constraint for a column that has the same name as the table.
    cur.execute(
        dedent(f"""
        ALTER TABLE {table_name} ADD CONSTRAINT {table_name}_const
        CHECK ({table_name} > 0);
        """)
    )
    if with_exclusion_constraint:
        cur.execute(
            dedent(f"""
            ALTER TABLE {table_name}
            ADD CONSTRAINT exclusion_var
            EXCLUDE USING BTREE (var_maybe_with_exclusion WITH =);
            """)
        )

    cur.execute(
        dedent(f"""
        INSERT INTO {table_name} (
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
            {table_name},
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
            (floor(random() * {referred_table_rows}) + 1)::int,
            (floor(random() * {referred_table_rows}) + 1)::int,
            (floor(random() * 10) + 1)::int,
            substring(md5(random()::text), 1, 10),
            substring(md5(random()::text), 1, 10)
        FROM generate_series(1, {rows});
    """)
    )
    cur.execute(
        dedent(f"""
        CREATE TABLE referring_table (
            id SERIAL PRIMARY KEY,
            {table_name}_id INTEGER REFERENCES {table_name}(id)
        );
    """)
    )
    cur.execute(
        dedent(f"""
        INSERT INTO referring_table ({table_name}_id)
        SELECT generate_series(1, {referring_table_rows});
    """)
    )
    cur.execute(
        dedent(f"""
        CREATE TABLE not_valid_referring_table (
            id SERIAL PRIMARY KEY,
            {table_name}_id INTEGER
        );
    """)
    )
    cur.execute(
        dedent(f"""
        ALTER TABLE not_valid_referring_table ADD CONSTRAINT not_valid_referring
            FOREIGN KEY ({table_name}_id)
            REFERENCES {table_name}(id)
            NOT VALID;
    """)
    )
    cur.execute(
        dedent(f"""
        INSERT INTO not_valid_referring_table ({table_name}_id)
        SELECT generate_series(1, {referring_table_rows});
    """)
    )
