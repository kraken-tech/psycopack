"""
A customizable way to repack a table using psycopg.
"""

from ._conn import get_db_connection
from ._cur import get_cursor
from ._introspect import BackfillBatch
from ._partition import (
    DateRangeStrategy,
    NumericRangeStrategy,
    PartitionConfig,
    PartitionInterval,
)
from ._registry import RegistryException, UnexpectedSyncStrategy
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
    PartitioningForTableWithReferringFKs,
    PostBackfillBatchCallback,
    PrimaryKeyNotFound,
    Psycopack,
    ReferringForeignKeyInDifferentSchema,
    TableDoesNotExist,
    TableHasTriggers,
    TableIsEmpty,
    UnsupportedPrimaryKey,
)
from ._sync_strategy import SyncStrategy
from ._tracker import FailureDueToLockTimeout, Stage


__all__ = (
    "BackfillBatch",
    "BasePsycopackError",
    "CompositePrimaryKey",
    "DateRangeStrategy",
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
    "NumericRangeStrategy",
    "PartitionConfig",
    "PartitionInterval",
    "PartitioningForTableWithReferringFKs",
    "PostBackfillBatchCallback",
    "PrimaryKeyNotFound",
    "Psycopack",
    "ReferringForeignKeyInDifferentSchema",
    "RegistryException",
    "Stage",
    "SyncStrategy",
    "TableDoesNotExist",
    "TableHasTriggers",
    "TableIsEmpty",
    "UnexpectedSyncStrategy",
    "UnsupportedPrimaryKey",
    "get_cursor",
    "get_db_connection",
)
