from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Config:
    sql_file: str
    service: str
    default_database: str
    default_schema: str
    openmetadata_url: str
    token: str
    target: Optional[str]
    dry_run: bool
