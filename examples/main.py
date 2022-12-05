from uuid import UUID
from cql_orm import UserDefinedType, Timestamp, Text, Int, Set, Decimal
from cql_orm import ClusteringKeyColumn, PartitionKeyColumn, Column, Ordering, get_base_table

BASE = get_base_table()


class Employee(UserDefinedType):
    employee_num = Int
    name = Text
    surname = Text
    phones = Set(Text, frozen=True)


class Address(UserDefinedType):
    street = Text
    city = Text
    state = Text


class Sensor(BASE):
    id = PartitionKeyColumn(UUID)
    capture = ClusteringKeyColumn(Timestamp, ordering=Ordering.Ascending)
    installation = Column(Address)
    contact = Column(Set(Employee, frozen=True))
    temperature = Column(Decimal)
    humidity = Column(Decimal)


BASE.create_all("sensor_measurements")
