from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field, field_validator

class User(BaseModel):
    id: int
    email: str
    first_name: str
    last_name: str
    avatar: Optional[str] = None
    source: str = "api"
    department: Optional[str] = None
    hire_date: Optional[datetime] = None
    salary: Optional[float] = None
    is_active: bool = True

    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: str) -> str:
        return v.strip().lower()

    @field_validator("first_name", "last_name", mode="before")
    @classmethod
    def strip_strings(cls, v: str) -> str:
        return v.strip() if isinstance(v, str) else v

    @field_validator("hire_date", mode="before")
    @classmethod
    def parse_hire_date(cls, v):
        if v is None or v == "":
            return None
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        for fmt in ("%Y-%m-%dT%H:%M:%S%z","%Y-%m-%dT%H:%M:%S","%Y-%m-%d %H:%M:%S%z","%Y-%m-%d %H:%M:%S","%Y-%m-%d"):
            try:
                dt = datetime.strptime(str(v), fmt)
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        try:
            dt = datetime.fromisoformat(str(v))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
