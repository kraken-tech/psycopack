"""
A customizable way to repack a table using psycopg.
"""

from ._conn import get_db_connection
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
    PrimaryKeyNotFound,
    Psycopack,
    ReferringForeignKeyInDifferentSchema,
    TableDoesNotExist,
    TableHasTriggers,
    TableIsEmpty,
    UnsupportedPrimaryKey,
)
from ._tracker import FailureDueToLockTimeout


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
    "TableDoesNotExist",
    "TableHasTriggers",
    "TableIsEmpty",
    "UnsupportedPrimaryKey",
    "get_db_connection",
)
