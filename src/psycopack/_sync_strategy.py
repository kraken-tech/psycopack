import enum


class SyncStrategy(enum.Enum):
    # The DIRECT_TRIGGER strategy uses a trigger to sync data between the
    # source table and the copy table. As a consequence, the schema
    # synchronisation is done CONCURRENTLY.
    DIRECT_TRIGGER = "DIRECT_TRIGGER"
    # The CHANGE_LOG strategy uses a trigger to push data from the source
    # table onto a table that logs the row changes during/after backfilling. In
    # this case, the schema synchronisation can be done without CONCURRENT DDLs
    # being necessary.
    CHANGE_LOG = "CHANGE_LOG"
