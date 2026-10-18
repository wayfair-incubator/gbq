import re
from enum import Enum
from typing import TypeVar

from pydantic import BaseModel, Field, ValidationError, model_validator


class StructureType(Enum):
    table = "table"
    view = "view"
    stored_procedure = "stored_procedure"


class PartitionType(Enum):
    time = "time"
    range = "range"


class TimeType(Enum):
    DAY = "DAY"
    HOUR = "HOUR"
    MONTH = "MONTH"
    YEAR = "YEAR"


class BigQueryDataType(Enum):
    TYPE_KIND_UNSPECIFIED = "TYPE_KIND_UNSPECIFIED"
    INT64 = "INT64"
    BOOL = "BOOL"
    FLOAT64 = "FLOAT64"
    STRING = "STRING"
    BYTES = "BYTES"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    GEOGRAPHY = "GEOGRAPHY"
    NUMERIC = "NUMERIC"
    BIGNUMERIC = "BIGNUMERIC"
    JSON = "JSON"
    INTERVAL = "INTERVAL"
    ARRAY = "ARRAY"
    STRUCT = "STRUCT"


E = TypeVar("E", bound=Enum)


def _enum_member(enum_cls: type[E], name: str) -> E:
    """
    Function looks up an enum member by name, raising ValueError (which pydantic reports
    as a ValidationError) instead of KeyError when the name is not valid.
    """
    try:
        return enum_cls[name]
    except KeyError:
        valid = ", ".join(enum_cls.__members__)
        raise ValueError(
            f"Invalid {enum_cls.__name__} '{name}'. Expected one of: {valid}"
        ) from None


class TimeDefinition(BaseModel):
    type: TimeType
    expirationMs: str | None = None
    field: str | None = None

    @model_validator(mode="before")
    @classmethod
    def str_or_list_(cls, data):
        if isinstance(data, dict) and isinstance(data.get("type"), str):
            data = {**data, "type": _enum_member(TimeType, data["type"].upper())}
        return data


class RangeFieldDefinition(BaseModel):
    start: int
    end: int
    interval: int


class RangeDefinition(BaseModel):
    field: str
    range: RangeFieldDefinition


class Partition(BaseModel):
    type: PartitionType
    definition: TimeDefinition | RangeDefinition

    @model_validator(mode="before")
    @classmethod
    def convert_type(cls, data):
        if isinstance(data, dict) and isinstance(data.get("type"), str):
            data = {**data, "type": _enum_member(PartitionType, data["type"].lower())}
        return data


class SqlType(BaseModel):
    """
    A BigQuery standard SQL type, including the element type of an ARRAY and the
    fields of a STRUCT.
    """

    type_kind: BigQueryDataType
    array_element_type: "SqlType | None" = None
    struct_fields: "list[SqlStructField] | None" = None

    @model_validator(mode="after")
    def check_nested_types(self):
        if self.type_kind == BigQueryDataType.ARRAY:
            if self.array_element_type is None:
                raise ValueError("ARRAY needs an element type, e.g. ARRAY<INT64>")
            if self.array_element_type.type_kind == BigQueryDataType.ARRAY:
                raise ValueError("BigQuery does not support an ARRAY of ARRAY")
        if self.type_kind == BigQueryDataType.STRUCT and not self.struct_fields:
            raise ValueError("STRUCT needs fields, e.g. STRUCT<id INT64, name STRING>")
        return self


class SqlStructField(BaseModel):
    name: str | None = None
    type: SqlType


# Resolve SqlType's forward reference to SqlStructField
SqlType.model_rebuild()


# Type names, field names and the punctuation of ARRAY<...> and STRUCT<...>
_TYPE_TOKEN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|[<>,]|\S")


def parse_sql_type(text: str) -> SqlType:
    """
    Function parses a BigQuery type such as "INT64", "ARRAY<STRING>" or
    "STRUCT<id INT64, tags ARRAY<STRING>>". Type names are case-insensitive, and STRUCT
    field names are optional.

    Raises:
        ValueError: If the text is not a supported type.
    """
    tokens = _TYPE_TOKEN.findall(text)
    position = 0

    def peek() -> str | None:
        return tokens[position] if position < len(tokens) else None

    def take(expected: str | None = None) -> str:
        nonlocal position
        word = peek()
        if word is None or (expected is not None and word != expected):
            found = f"'{word}'" if word is not None else "the end"
            wanted = f"'{expected}'" if expected else "a type"
            raise ValueError(f"Invalid type '{text}': expected {wanted}, found {found}")
        position += 1
        return word

    def is_name(word: str | None) -> bool:
        return word is not None and word not in "<>," and word[0].isalpha()

    def parse_type() -> SqlType:
        word = take()
        if not is_name(word):
            raise ValueError(f"Invalid type '{text}': unexpected '{word}'")
        kind = _enum_member(BigQueryDataType, word.upper())

        if kind == BigQueryDataType.ARRAY and peek() == "<":
            take("<")
            element = parse_type()
            take(">")
            return SqlType(type_kind=kind, array_element_type=element)

        if kind == BigQueryDataType.STRUCT and peek() == "<":
            take("<")
            fields = [parse_field()]
            while peek() == ",":
                take(",")
                fields.append(parse_field())
            take(">")
            return SqlType(type_kind=kind, struct_fields=fields)

        return SqlType(type_kind=kind)

    def parse_field() -> SqlStructField:
        # "name TYPE" when two names follow each other, otherwise an unnamed "TYPE"
        if is_name(peek()) and is_name(
            tokens[position + 1] if position + 1 < len(tokens) else None
        ):
            name = take()
            return SqlStructField(name=name, type=parse_type())
        return SqlStructField(type=parse_type())

    try:
        sql_type = parse_type()
    except ValidationError as e:
        # Surface the model's own message (e.g. a missing ARRAY element type)
        message = e.errors()[0]["msg"].removeprefix("Value error, ")
        raise ValueError(f"Invalid type '{text}': {message}") from None
    if peek() is not None:
        raise ValueError(f"Invalid type '{text}': unexpected '{peek()}'")
    return sql_type


class Argument(BaseModel):
    name: str
    # The top-level kind, e.g. ARRAY for "ARRAY<INT64>"
    data_type: BigQueryDataType
    # The full type, including ARRAY element types and STRUCT fields
    sql_type: SqlType

    @model_validator(mode="before")
    @classmethod
    def str_or_list_(cls, data):
        if not isinstance(data, dict) or data.get("sql_type") is not None:
            return data
        data_type = data.get("data_type")
        if isinstance(data_type, str):
            sql_type = parse_sql_type(data_type)
        elif isinstance(data_type, BigQueryDataType):
            sql_type = SqlType(type_kind=data_type)
        else:
            return data
        return {**data, "data_type": sql_type.type_kind, "sql_type": sql_type}


class Structure(BaseModel):
    table_schema: list[dict] = Field([], alias="schema")
    partition: Partition | None = None
    clustering: list[str] | None = None
    labels: dict[str, str] = Field({})
    description: str | None = None
    view_query: str | None = None
    body: str | None = None
    type: StructureType | None = None
    arguments: list[Argument] | None = None

    @model_validator(mode="before")
    @classmethod
    def validate_type(cls, data):
        if isinstance(data, dict) and not data.get("type", None):
            if data.get("view_query"):
                data = {**data, "type": StructureType.view}
            elif data.get("body"):
                data = {**data, "type": StructureType.stored_procedure}
            else:
                data = {**data, "type": StructureType.table}
        return data

    @model_validator(mode="before")
    @classmethod
    def str_or_list_body(cls, data):
        if (
            isinstance(data, dict)
            and isinstance(data.get("body"), list)
            and not [s for s in data.get("body") if not isinstance(s, str)]
        ):
            data = {**data, "body": "\n".join(data["body"])}
        return data
