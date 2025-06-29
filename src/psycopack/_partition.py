import dataclasses
import typing


@dataclasses.dataclass
class DateRangeStrategy:
    partition_by: typing.Literal["DAY", "MONTH"]


@dataclasses.dataclass
class PartitionConfig:
    column: str
    num_of_extra_partitions_ahead: int
    # Todo: Add support for other types of partitioning.
    strategy: DateRangeStrategy
