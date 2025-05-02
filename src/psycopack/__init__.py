"""
A customizable way to repack a table using psycopg.
"""

from ._repack import (
    BackfillBatch,
    BaseRepackError,
    CompositePrimaryKey,
    InheritedTable,
    InvalidPrimaryKeyTypeForConversion,
    InvalidStageForReset,
    PrimaryKeyNotFound,
    Repack,
    TableDoesNotExist,
    TableHasTriggers,
    TableIsEmpty,
    UnsupportedPrimaryKey,
)
from ._tracker import FailureDueToLockTimeout


__all__ = (
    "BackfillBatch",
    "BaseRepackError",
    "CompositePrimaryKey",
    "FailureDueToLockTimeout",
    "InheritedTable",
    "InvalidPrimaryKeyTypeForConversion",
    "InvalidStageForReset",
    "PrimaryKeyNotFound",
    "Repack",
    "TableDoesNotExist",
    "TableHasTriggers",
    "TableIsEmpty",
    "UnsupportedPrimaryKey",
)
