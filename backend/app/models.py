from pydantic import BaseModel
from typing import List, Any

class PreviewResponse(BaseModel):
    columns: List[str]
    data: List[List[Any]]

class ImputeRequest(BaseModel):
    method: str  # 'mean', 'median', 'mode', or 'constant'
    columns: List[str]
    value: Any = None  # Used if method == 'constant'

class EncodeRequest(BaseModel):
    method: str  # 'onehot' or 'ordinal'
    columns: List[str]

class ScaleRequest(BaseModel):
    method: str  # 'minmax' or 'standard'
    columns: List[str]

class DropColumnsRequest(BaseModel):
    columns: List[str]

class FilterRowsRequest(BaseModel):
    column: str
    value: Any = None  # for exact match
    min_value: Any = None  # for range
    max_value: Any = None  # for range
    regex: str = None  # for regex match

class RenameColumnsRequest(BaseModel):
    rename_map: dict[str, str]

class ChangeDtypesRequest(BaseModel):
    dtype_map: dict[str, str]

class DropDuplicatesRequest(BaseModel):
    subset: list[str] = None