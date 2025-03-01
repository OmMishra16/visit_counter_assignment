from pydantic import BaseModel, Field
from typing import Dict, List, Any

class VisitCount(BaseModel):
    visits: int
    served_via: str

