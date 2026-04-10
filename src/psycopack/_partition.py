import dataclasses
import enum


class PartitionInterval(enum.Enum):
    DAY = "DAY"
    MONTH = "MONTH"


@dataclasses.dataclass
class DateRangeStrategy:
    partition_by: PartitionInterval


@dataclasses.dataclass
class NumericRangeStrategy:
    range_size: int


@dataclasses.dataclass
class PartitionConfig:
    column: str
    num_of_extra_partitions_ahead: int
    strategy: DateRangeStrategy | NumericRangeStrategy
