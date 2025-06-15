import dataclasses
import typing


@dataclasses.dataclass
class DateRangeStrategy:
    partition_by: typing.Literal["DAY", "MONTH"]


class PartitionConfig:
    def __init__(
        self,
        *,
        column: str,
        num_of_extra_partitions_ahead: int,
        # TODO: Add support for other types of strategies.
        strategy: DateRangeStrategy,
    ) -> None:
        pass
