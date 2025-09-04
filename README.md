# Psycopack

Customisable table reorganising using shadow tables and minimal locks.

Psycopack can be used to perform the following operations without downtime:

- Schema changes that would be dangerous (locking the table for too long) using
  regular SQL DDL statements (e.g. adding exclusion constraints).
- Converting a primary key from int to bigint.
- Clustering (or repacking) the table to address table and index bloat (similar
  to `pg_repack`).
- TODO: partition an existing table.

## Basics

Psycopack uses a shadow-table approach.

This means creating a copy of the table (shadow table), synchronising its data
with the original table data, and swapping it with the original.

The Psycopack process achieves the above in 6 major steps:

1. **Pre-validation**: Checks if the table can be processed by Psycopack at
   all. See _Known Limitations_ below.
2. **Setup**: Creates the copy table, a trigger to keep the shadow table in
   sync with the original, and a backfill log table to keep track of rows from
   the original table that need to be copied over.
3. **Backfill**: Copies data from the original table to the copy one.
4. **Schema synchronisation**: Recreates all indexes and constraints in the
   copy table. Also recreates foreign keys on referring tables. This step is
   performed after the backfill is complete to speed up writes on the copy
   table.
5. **Swap**: Effectively swaps the original table by the copy table. The
   original (now old) table will be kept in sync via triggers, in case the swap
   process needs to be reverted.
6. **Clean up**: Drops the old table.

## Python Interface

Psycopack runs on a Python class that can be used to configure the outcome of
the process. Below are some examples of how `Psycopack` can be used.

### Example: Performing schema changes

```py
from psycopack import psycopack, get_db_connection

with get_db_connection("postgresql://user:password@host:port/db") as conn:
    with conn.cursor() as cur:
        psycopack = Psycopack(
            batch_size=50_000,
            conn=conn,
            cur=cur,
            schema="public",
            table="to_psycopack",
        )
        psycopack.pre_validate()
        psycopack.setup_repacking()

        # Now that the shadow table is set up, change the schema accordingly.
        cur.execute(f"ALTER TABLE {psycopack.copy_table} ... ")

        # Finish the process.
        psycopack.sync_schemas()
        psycopack.swap()
        psycopack.clean_up()
```

### Example: Converting the primary key to bigint

```py
from psycopack import psycopack, get_db_connection

with get_db_connection("postgresql://user:password@host:port/db") as conn:
    with conn.cursor() as cur:
        psycopack = Psycopack(
            batch_size=50_000,
            conn=conn,
            cur=cur,
            schema="public",
            table="to_psycopack",
            # Set this argument and Psycopack will do the conversion
            # automatically.
            convert_pk_to_bigint=True,
        )
        psycopack.full()
```

### Example: Repacking (or clustering)

```python

from psycopack import psycopack, get_db_connection

with get_db_connection("postgresql://user:password@host:port/db") as conn:
    with conn.cursor() as cur:
        psycopack = Psycopack(
            batch_size=50_000,
            conn=conn,
            cur=cur,
            schema="public",
            table="to_psycopack",
        )
        # Simply run `.full()` to repack the whole table with further
        # customisation.
        psycopack.full()
```

## Known Limitations

The following types of tables aren't currently supported:

- Inherited tables.
- Tables without primary keys.
- Tables with composite primary keys.
- Primary key field type not one of:
  - `bigint`
  - `bigserial`
  - `integer`
  - `serial`
  - `smallint`
  - `smallserial`
- Tables with triggers.
- Tables with invalid indexes (the user should drop or re-index them first).
- Tables with deferrable unique constraints.
- Referring foreign keys on a different schema than the original table.

## Required user permissions (or privileges)

Unless the user is a superuser, they may lack certain privileges to run
Psycopack.

If such privileges are not assigned, `Psycopack` will raise an error informing
the user which tables are lacking a particular privilege or ownership and which
statements are required to fix it.

In summary, the user must have **at least**:

- CREATE and USAGE privilege on the schema:
  ```sql
  GRANT CREATE, USAGE ON SCHEMA <schema> TO <user>;
  ```
- Ownership of the original table **and** tables with foreign keys pointing
  to the original table:
  ```sql
  ALTER TABLE <table> OWNER TO <user>;
  ```
- REFERENCES privilege on tables the original table has foreign keys to:
 ```sql
 GRANT REFERENCES ON TABLE <table> TO <user>;
 ```
