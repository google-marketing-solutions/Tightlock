from typing import Dict, Optional
from datetime import datetime

from sqlmodel import SQLModel, Field, JSON, Column, DateTime


class Activation(SQLModel):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str # Activation name
    source_name: str # Source name
    destination_name: str # Destination name
    schedule: Optional[str] = None # A cron expression or a cron preset (None for triggered)"


class Config(SQLModel):
    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp: datetime = Field(sa_column=Column(DateTime(timezone=True)), default_factory=datetime.now().timestamp(), nullable=False)
    label: str
    value: Dict = Field(default={}, sa_column=Column(JSON))

    # Needed for Column(JSON)
    class Config: # Inner `Config`` class is needed by sqlmodel. Same name as the parant class is a coincidence.
        arbitrary_types_allowed = True