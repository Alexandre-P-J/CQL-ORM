class BaseType:
    def __init__(self, frozen=False):
        self.frozen = frozen

    def typename(self) -> str:
        return f"frozen<{self.name}>" if self.frozen else self.name

    def definition(self) -> str:
        self.typename()

    def __str__(self) -> str:
        return self.typename()


class Ascii(BaseType):
    name = "ascii"


class BigInt(BaseType):
    name = "bigint"


class Blob(BaseType):
    name = "blob"

    def __init__(self):
        super().__init__(False)


class Boolean(BaseType):
    name = "boolean"


class Counter(BaseType):
    name = "counter"


class Decimal(BaseType):
    name = "decimal"


class Double(BaseType):
    name = "double"


class Float(BaseType):
    name = "float"


class Inet(BaseType):
    name = "inet"


class Int(BaseType):
    name = "int"


class List(BaseType):
    name = "list"

    def __init__(self, item_type: BaseType, frozen=False):
        item_type = item_type() if isinstance(item_type, type) else item_type
        if not isinstance(item_type, BaseType):
            raise ValueError("List items must be of a valid type")
        super().__init__(frozen)
        self.item_type = item_type

    def typename(self):
        it = self.item_type.typename()
        return f"frozen<{self.name}<{it}>>" if self.frozen else f"{self.name}<{it}>"


class Map(BaseType):
    name = "map"

    def __init__(self, key_type: BaseType, value_type: BaseType, frozen=False):
        key_type = key_type() if isinstance(key_type, type) else key_type
        value_type = value_type() if isinstance(value_type, type) else value_type
        if not isinstance(key_type, BaseType) or not isinstance(value_type, BaseType):
            raise ValueError("Map key and value must be of a valid type")
        super().__init__(frozen)
        self.key_type = key_type
        self.value_type = value_type

    def typename(self):
        kt = self.key_type.typename()
        vt = self.value_type.typename()
        return f"frozen<{self.name}<{kt},{vt}>>" if self.frozen else f"{self.name}<{kt},{vt}>"


class Set(BaseType):
    name = "set"

    def __init__(self, item_type: BaseType, frozen=False):
        item_type = item_type() if isinstance(item_type, type) else item_type
        if not isinstance(item_type, BaseType):
            raise ValueError("Set items must be of a valid type")
        super().__init__(frozen)
        self.item_type = item_type

    def typename(self):
        it = self.item_type.typename()
        return f"frozen<{self.name}<{it}>>" if self.frozen else f"{self.name}<{it}>"


class Text(BaseType):
    name = "text"


class Timestamp(BaseType):
    name = "timestamp"


class TimeUUID(BaseType):
    name = "timeuuid"

# class Tuple(BaseType):
#     name = "tuple"

#     def __init__(self, frozen):
#         self.frozen = frozen


class UUID(BaseType):
    name = "uuid"


class VarChar(BaseType):
    name = "varchar"


class VarInt(BaseType):
    name = "varint"
