from cql_orm.core.types import BaseType
from collections import OrderedDict
from dataclasses import dataclass
from cql_orm.core.columns import Column
from cassandra.cluster import Session


def get_public_fields(obj):
    if not isinstance(obj, type):
        raise ValueError(f"{obj} is not a class")
    elif hasattr(obj, '__dict__'):
        fields = obj.__dict__.keys()
    elif hasattr(obj, '__slots__'):
        fields = obj.__slots__
    else:
        fields = dir(obj)
    return [field for field in fields if not field.startswith('_')]


@dataclass
class TableSettings:
    name: str
    columns: OrderedDict


class UserDefinedType(BaseType):
    _types = OrderedDict()

    @classmethod
    def __init_subclass__(cls) -> None:
        fields = OrderedDict()
        for field in get_public_fields(cls):
            value = getattr(cls, field)
            value = value() if isinstance(value, type) else value
            if not isinstance(value, BaseType):
                raise Exception(
                    f"User defined type attribute {field} must be of a valid type")
            fields[field] = value
        UserDefinedType._types[cls] = fields

    def typename(self):
        if not issubclass(self.__class__, UserDefinedType):
            raise Exception("User defined type must be of a valid type")
        name = self.__class__.__name__
        return f"frozen<{name}>" if self.frozen else name

    def definition(self) -> str:
        if not issubclass(self.__class__, UserDefinedType):
            raise Exception("User defined type must be of a valid type")
        attributes = ""
        for field_name, field in self._types[self.__class__].items():
            attr = f"{field_name} {field.typename()}"
            attributes += f", {attr}" if attributes else attr
        return f"{self.__class__.__name__}({attributes})"


def get_base_table():
    class CassandraTable:
        _tables = []
        _names = set()

        @classmethod
        def __get_table_name(cls, table_class):
            if hasattr(table_class, "__tablename__"):
                return getattr(table_class, "__tablename__")
            return table_class.__name__

        @classmethod
        def __init_subclass__(cls) -> None:
            columns = OrderedDict()
            for column in get_public_fields(cls):
                value = getattr(cls, column)
                value = value() if isinstance(value, type) else value
                if not isinstance(value, Column):
                    raise Exception(
                        f"Column {column} must be of a valid column type")
                columns[column] = value
            name = cls.__get_table_name(cls)
            if name in cls._names:
                raise Exception(
                    f"Table {name} conflicts a table with the same name")
            cls._names.add(name)
            CassandraTable._tables.append(TableSettings(name, columns))

        @classmethod
        def create_all(cls, keyspace: str, session: Session):
            if cls is not CassandraTable:
                raise NotImplementedError("Method can't be called by table")
            from cql_orm.core.cql import InitStatements
            InitStatements.create_if_not_exists(keyspace, cls._tables, session)
    return CassandraTable
