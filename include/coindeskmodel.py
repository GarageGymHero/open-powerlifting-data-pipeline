from typing import Dict
from pydantic import BaseModel

class CoindeskModel(BaseModel):
    time: Dict[str,str]
    disclaimer: str
    bpi: Dict[str, Dict[str,str]]