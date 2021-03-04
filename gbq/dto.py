from enum import Enum
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field, root_validator, validator


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
    expirationMs: Optional[str]
    field: Optional[str]


class RangeFieldDefinition(BaseModel):
    start: int
    end: int
    interval: int


class RangeDefinition(BaseModel):
    field: str
    range: RangeFieldDefinition


class Partition(BaseModel):
    type: PartitionType
    definition: Union[TimeDefinition, RangeDefinition]


class Argument(BaseModel):
    name: str
    data_type: BigQueryDataType

    @validator("data_type", pre=True)
    def str_or_list_(cls, v):
        return v.upper()


class Structure(BaseModel):
    table_schema: List[Dict] = Field([], alias="schema")
    partition: Union[Partition, None] = None
    clustering: Union[List[str], None] = None
    labels: Dict[str, str] = Field({})
    description: Union[str, None] = None
    view_query: Union[str, None] = None
    body: Union[str, None] = None
    type: Union[StructureType, None] = None
    arguments: Union[List[Argument], None] = None

    @root_validator
    def validate_type(cls, values):
        if not values.get("type", None):
            if values["view_query"]:
                values["type"] = StructureType.view
            elif values["body"]:
                values["type"] = StructureType.stored_procedure
            else:
                values["type"] = StructureType.table
        return values

    @validator("view_query", "body", pre=True)
    def str_or_list_(cls, v):
        if isinstance(v, list) and not [s for s in v if not isinstance(s, str)]:
            v = "\n".join(v)
        return v
