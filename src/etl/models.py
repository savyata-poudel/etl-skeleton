from pydantic import BaseModel, Field, field_validator
from typing import Optional

class Customer(BaseModel):
    customer_id: str
    name: str
    email: Optional[str] = None
    Updated_at:str = Field(..., description="ISO8601")

    @field_validator('email')
    @classmethod
    def normalize_email(cls, v):
        if v:
            return v.lower()
        return v