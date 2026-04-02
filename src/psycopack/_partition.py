import dataclasses
import enum


class PartitionInterval(enum.Enum):
    DAY = "DAY"
    MONTH = "MONTH"


@dataclasses.dataclass
class DateRangeStrategy:
    partition_by: PartitionInterval


@dataclasses.dataclass
class PartitionConfig:
    column: str
    num_of_extra_partitions_ahead: int
    # Todo: Add support for other types of partitioning.
    strategy: DateRangeStrategy
