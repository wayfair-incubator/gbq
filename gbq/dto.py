from enum import Enum
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field


class StructureType(Enum):
    table = "table"
    view = "view"


class PartitionType(Enum):
    time = "time"
    range = "range"


class TimeType(Enum):
    DAY = "DAY"
    HOUR = "HOUR"
    MONTH = "MONTH"
    YEAR = "YEAR"


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


class Structure(BaseModel):
    type: StructureType = StructureType.table
    project_id: Union[str, None] = None
    dataset_id: Union[str, None] = None
    structure_id: Union[str, None] = None
    table_schema: List[Dict] = Field([], alias="schema")
    partition: Union[Partition, None] = None
    clustering: Union[List[str], None] = None
    labels: Dict[str, str] = Field({})
    description: Union[str, None] = None
    view_query: Union[str, None] = None
