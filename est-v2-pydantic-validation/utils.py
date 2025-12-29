from typing import Dict, List, Any, Optional
from pydantic import BaseModel, ValidationError, field_validator

class Line(BaseModel):
    ID: int
    Description: str
    Operation: str
    Reason: str
    RepairHours: Optional[float] = None
    RepairCost: Optional[float] = None
    
class Payload(BaseModel):
    Lines: List[Line]

    # runs before the built-in parsing/coercion of `Lines`
    @field_validator("Lines", mode="before")
    @classmethod
    def _filter_invalid_lines(cls, raw_list: Any) -> List[Any]:
        cleaned: List[Line] = []
        for entry in raw_list or []:
            try:
                # use model_validate to parse/coerce a single Line
                cleaned.append(Line.model_validate(entry))
            except ValidationError:
                # drop anything that doesn’t validate
                continue
        return cleaned

def validate_model(model_response):
    payload = Payload.model_validate(model_response)
    result = payload.model_dump()   
    return result

    