from enum import Enum
from cql_orm.core.types import BaseType


class Ordering(Enum):
    Descending = "DESC"
    Ascending = "ASC"
    Undefined = "UNDEFINED"


class Column:
    def __init__(self, datatype: BaseType, static=False) -> None:
        self.datatype = datatype() if isinstance(datatype, type) else datatype
        self.static = static


class PartitionKeyColumn(Column):
    def __init__(self, datatype: BaseType) -> None:
        super().__init__(datatype, False)


class ClusteringKeyColumn(Column):
    def __init__(self, datatype: BaseType, ordering: Ordering = Ordering.Undefined,
                 static: bool = False) -> None:
        self.ordering = ordering
        super().__init__(datatype, static)
