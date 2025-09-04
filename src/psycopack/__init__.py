"""
A customizable way to repack a table using psycopg.
"""

from ._conn import get_db_connection
from ._cur import get_cursor
from ._introspect import BackfillBatch
from ._repack import (
    BasePsycopackError,
    CompositePrimaryKey,
    DeferrableUniqueConstraint,
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
    "DeferrableUniqueConstraint",
    "FailureDueToLockTimeout",
    "InheritedTable",
    "InvalidIndexes",
    "InvalidPrimaryKeyTypeForConversion",
    "InvalidStageForReset",
    "NoCreateAndUsagePrivilegeOnSchema",
    "NoReferencesPrivilege",
    "NoReferringTableOwnership",
    "NotTableOwner",
    "PostBackfillBatchCallback",
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
