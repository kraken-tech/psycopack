"""
A customizable way to repack a table using psycopg.
"""

from ._conn import get_db_connection
from ._cur import get_cursor
from ._introspect import BackfillBatch
from ._repack import (
    BasePsycopackError,
    CompositePrimaryKey,
    InheritedTable,
    InvalidIndexes,
    InvalidPrimaryKeyTypeForConversion,
    InvalidStageForReset,
    NoCreateAndUsagePrivilegeOnSchema,
    NoReferencesPrivilege,
    NoReferringTableOwnership,
    NotTableOwner,
    PostBackfillBatchCallback,
    PrimaryKeyNotFound,
    Psycopack,
    ReferringForeignKeyInDifferentSchema,
    TableDoesNotExist,
    TableHasTriggers,
    TableIsEmpty,
    UnsupportedPrimaryKey,
)
from ._tracker import FailureDueToLockTimeout, Stage


__all__ = (
    "BackfillBatch",
    "BasePsycopackError",
    "CompositePrimaryKey",
    "FailureDueToLockTimeout",
    "InheritedTable",
    "InvalidIndexes",
    "InvalidPrimaryKeyTypeForConversion",
    "InvalidStageForReset",
    "NoCreateAndUsagePrivilegeOnSchema",
    "NoReferencesPrivilege",
    "NoReferringTableOwnership",
    "NotTableOwner",
    "PrimaryKeyNotFound",
    "ReferringForeignKeyInDifferentSchema",
    "Psycopack",
    "Stage",
    "TableDoesNotExist",
    "TableHasTriggers",
    "TableIsEmpty",
    "UnsupportedPrimaryKey",
    "get_cursor",
    "get_db_connection",
)
