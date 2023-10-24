from enum import Enum
from typing import Dict, List, Optional, Union

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
    expirationMs: Optional[str] = None
    field: Optional[str] = None

    @model_validator(mode="before")
    def str_or_list_(self):
        if isinstance(self.get("type"), str):
            self["type"] = TimeType[self.get("type", "").upper()]
        return self


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

    @model_validator(mode="before")
    def str_or_list_(self):
        if isinstance(self.get("data_type"), str):
            self["data_type"] = BigQueryDataType[self.get("data_type", "").upper()]
        return self


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

    @model_validator(mode="before")
    def validate_type(self):
        if not self.get("type", None):
            if self.get("view_query"):
                self["type"] = StructureType.view
            elif self.get("body"):
                self["type"] = StructureType.stored_procedure
            else:
                self["type"] = StructureType.table
        return self

    @model_validator(mode="before")
    def str_or_list_(self):
        if isinstance(self["body"], list) and not [s for s in self["body"] if not isinstance(s, str)]:
            self["body"] = "\n".join(self["body"])
        return self
