from enum import Enum

from pydantic import BaseModel, Field, model_validator


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
    ARRAY = "ARRAY"
    STRUCT = "STRUCT"


class TimeDefinition(BaseModel):
    type: TimeType
    expirationMs: str | None = None
    field: str | None = None

    @model_validator(mode="before")
    @classmethod
    def str_or_list_(cls, data):
        if isinstance(data, dict) and isinstance(data.get("type"), str):
            data["type"] = TimeType[data.get("type", "").upper()]
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
            data["type"] = PartitionType[data.get("type", "").lower()]
        return data


class Argument(BaseModel):
    name: str
    data_type: BigQueryDataType

    @model_validator(mode="before")
    @classmethod
    def str_or_list_(cls, data):
        if isinstance(data, dict) and isinstance(data.get("data_type"), str):
            data["data_type"] = BigQueryDataType[data.get("data_type", "").upper()]
        return data


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
                data["type"] = StructureType.view
            elif data.get("body"):
                data["type"] = StructureType.stored_procedure
            else:
                data["type"] = StructureType.table
        return data

    @model_validator(mode="before")
    @classmethod
    def str_or_list_body(cls, data):
        if (
            isinstance(data, dict)
            and isinstance(data.get("body"), list)
            and not [s for s in data.get("body") if not isinstance(s, str)]
        ):
            data["body"] = "\n".join(data["body"])
        return data
