"""
A customizable way to repack a table using psycopg.
"""

from ._repack import (
    BaseRepackError,
    CompositePrimaryKey,
    InheritedTable,
    InvalidPrimaryKeyTypeForConversion,
    PrimaryKeyNotFound,
    Repack,
    TableDoesNotExist,
    TableHasTriggers,
    TableIsEmpty,
    UnsupportedPrimaryKey,
)


__all__ = (
    "BaseRepackError",
    "CompositePrimaryKey",
    "InheritedTable",
    "InvalidPrimaryKeyTypeForConversion",
    "PrimaryKeyNotFound",
    "Repack",
    "TableDoesNotExist",
    "TableHasTriggers",
    "TableIsEmpty",
    "UnsupportedPrimaryKey",
)
